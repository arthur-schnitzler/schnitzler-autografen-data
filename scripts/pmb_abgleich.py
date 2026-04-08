#!/usr/bin/env python3
"""
PMB-Abgleich und Datenaufbereitung für das Schnitzler-Briefverzeichnis.

Liest:
  - pmb-export/listperson.xml, listorg.xml  (PMB-Gesamtexport)
  - schnitzler_briefe_raw.json               (DLA Marbach Briefe)
  - person_gnd_map.json                      (DLA-ID → GND/Wikidata)
  - CUL Handlist B - Tabellenblatt1.csv      (Cambridge, optional)

Schreibt:
  data/indices/listperson.xml   Personen mit PMB-IDs
  data/indices/listorg.xml      Organisationen mit PMB-IDs
  data/editions/briefe.xml      Alle Briefe mit PMB-Verweisen
  output/review.csv             Mögliche Matches zur Prüfung
  output/import_to_pmb.csv      Fehlende Einträge → Google Sheet
"""

import csv
import json
import re
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import date
from pathlib import Path
from unicodedata import normalize
from xml.dom import minidom

BASE = Path(__file__).resolve().parent.parent
NS = {"tei": "http://www.tei-c.org/ns/1.0"}
TEI_NS = "http://www.tei-c.org/ns/1.0"


# ===================================================================
# PMB-Index aufbauen
# ===================================================================

def parse_pmb_persons(path):
    """PMB listperson.xml → lookup by GND, by name."""
    tree = ET.parse(path)
    by_gnd = {}
    by_name = defaultdict(list)

    for p in tree.findall(".//tei:person", NS):
        xml_id = p.get("{http://www.w3.org/XML/1998/namespace}id", "")
        pmb_id = xml_id.replace("person__", "") if xml_id else None
        if not pmb_id:
            continue

        forename = surname = ""
        for pn in p.findall("tei:persName", NS):
            if not pn.get("type"):
                forename = (pn.findtext("tei:forename", "", NS) or "").strip()
                surname = (pn.findtext("tei:surname", "", NS) or "").strip()
                break

        full_name = f"{surname}, {forename}".strip(", ")

        birth = death = ""
        b_el = p.find("tei:birth/tei:date", NS)
        if b_el is not None:
            birth = b_el.get("when-iso", "")
        d_el = p.find("tei:death/tei:date", NS)
        if d_el is not None:
            death = d_el.get("when-iso", "")

        gnd = ""
        for idno in p.findall("tei:idno", NS):
            if idno.get("subtype") == "gnd":
                gnd = (idno.text or "").strip().replace(
                    "https://d-nb.info/gnd/", "")
                break

        entry = dict(pmb_id=pmb_id, forename=forename, surname=surname,
                     full_name=full_name, birth=birth, death=death,
                     gnd=gnd, type="person")

        if gnd:
            by_gnd[gnd] = entry
        norm = normalize_name(full_name)
        if norm:
            by_name[norm].append(entry)

    return by_gnd, by_name


def parse_pmb_orgs(path):
    """PMB listorg.xml → lookup by GND, by name."""
    tree = ET.parse(path)
    by_gnd = {}
    by_name = defaultdict(list)

    for o in tree.findall(".//tei:org", NS):
        xml_id = o.get("{http://www.w3.org/XML/1998/namespace}id", "")
        pmb_id = xml_id.replace("org__", "") if xml_id else None
        if not pmb_id:
            continue

        name = (o.findtext("tei:orgName", "", NS) or "").strip()
        gnd = ""
        for idno in o.findall("tei:idno", NS):
            if idno.get("subtype") == "gnd":
                gnd = (idno.text or "").strip().replace(
                    "https://d-nb.info/gnd/", "")
                break

        entry = dict(pmb_id=pmb_id, name=name, gnd=gnd, type="org")
        if gnd:
            by_gnd[gnd] = entry
        norm = normalize_name(name)
        if norm:
            by_name[norm].append(entry)

    return by_gnd, by_name


# ===================================================================
# Quelldaten laden
# ===================================================================

def load_dla(json_path, gnd_map_path):
    """DLA-JSON → Personen-dict, Org-dict, Briefe-Liste."""
    data = json.loads(Path(json_path).read_text(encoding="utf-8"))
    gnd_map = json.loads(Path(gnd_map_path).read_text(encoding="utf-8"))

    persons = {}
    orgs = {}
    briefe = []

    for item in data:
        cat = item.get("category", "")

        # Personen aus personBy/personTo
        for fid, fname in [("personBy_id_mv", "personBy_display_mv"),
                           ("personTo_id_mv", "personTo_display_mv")]:
            ids = item.get(fid, [])
            names = item.get(fname, [])
            for i, pid in enumerate(ids):
                if pid in persons:
                    continue
                display = names[i] if i < len(names) else ""
                gi = gnd_map.get(pid, {})
                persons[pid] = dict(
                    dla_id=pid, display_name=display,
                    name_from_map=gi.get("name", ""),
                    gnd=gi.get("gnd"), wikidata=gi.get("wikidata"),
                    birth=gi.get("birth", ""), death=gi.get("death", ""),
                    type="person", source="DLA")

        # Orgs aus filterAuthority (enthalten <Ort>)
        for auth in item.get("filterAuthority_mv", []):
            if "<" in auth and ">" in auth and auth not in orgs:
                orgs[auth] = dict(
                    dla_id=auth, display_name=auth,
                    gnd=None, type="org", source="DLA")

        # Briefe sammeln
        if cat in ("Briefe von", "Briefe an", "Briefe anderer"):
            brief = dict(
                id=item["id"],
                display=item.get("display", ""),
                category=cat,
                date_iso=item.get("dateOriginStart", ""),
                date_display=item.get("displayAddition1", ""),
                date_range=item.get("filterDateRange_mv"),
                sender_ids=item.get("personBy_id_mv", []),
                sender_names=item.get("personBy_display_mv", []),
                receiver_ids=item.get("personTo_id_mv", []),
                receiver_names=item.get("personTo_display_mv", []),
                extent=item.get("extent", ""),
                url=item.get("url", ""),
                accession=item.get("accessionNumber", ""),
                source="DLA",
            )
            briefe.append(brief)

    return persons, orgs, briefe


def load_cul(csv_path):
    """CUL Handlist → Personen + Konvolut-Briefe."""
    persons = {}
    briefe = []

    if not Path(csv_path).exists():
        return persons, briefe

    with open(csv_path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            mappe = (row.get("Mappennummer") or "").strip()
            surname = (row.get("Nachname") or "").strip()
            forename = (row.get("Vorname") or "").strip()
            titel = (row.get("Titel_komplett") or "").strip()
            pmb = (row.get("PMB") or "").strip()

            cul_id = f"CUL_{mappe}"
            display = f"{surname}, {forename}"

            if display not in persons:
                persons[display] = dict(
                    dla_id=cul_id, display_name=display,
                    name_from_map="", gnd=None, wikidata=None,
                    birth="", death="",
                    pmb_id=pmb if pmb else None,
                    type="person", source="CUL")

            # Zeitraum aus Titel extrahieren (z.B. "1917-1930")
            date_range = ""
            m = re.search(r"(\d{4})-(\d{4})", titel)
            if m:
                date_range = f"{m.group(1)}/{m.group(2)}"

            briefe.append(dict(
                id=cul_id,
                display=titel,
                category="Briefe an",  # CUL = Briefe an Schnitzler
                date_iso="",
                date_display="",
                date_range=[date_range] if date_range else None,
                sender_ids=[], sender_names=[display],
                receiver_ids=["PE00000648"],
                receiver_names=["Schnitzler, Arthur (1862-1931)"],
                extent=mappe,
                url="",
                accession=mappe,
                source="CUL",
                pmb_known=pmb if pmb else None,
            ))

    return persons, briefe


# ===================================================================
# Normalisierung und Matching
# ===================================================================

def normalize_name(name):
    if not name:
        return ""
    name = normalize("NFC", name).lower()
    name = re.sub(r"\(.*?\)", "", name)
    name = re.sub(r"<.*?>", "", name)
    name = re.sub(r"[''`´]", "'", name)
    name = re.sub(r"\s+", " ", name)
    return name.strip(" ,.-;")


def extract_surname_forename(display_name):
    name = re.sub(r"\(.*?\)", "", display_name).strip()
    name = re.sub(r"<.*?>", "", name).strip()
    parts = name.split(",", 1)
    surname = parts[0].strip()
    forename = parts[1].strip() if len(parts) > 1 else ""
    return surname, forename


def dates_compatible(source, pmb):
    sb = (source.get("birth") or "")[:4]
    sd = (source.get("death") or "")[:4]
    pb = (pmb.get("birth") or "")[:4]
    pd = (pmb.get("death") or "")[:4]
    if sb and pb and sb != pb:
        return False
    if sd and pd and sd != pd:
        return False
    return True


def match_person(person, pmb_by_gnd, pmb_by_name):
    # 1. GND
    gnd = person.get("gnd")
    if gnd and gnd in pmb_by_gnd:
        return "matched", pmb_by_gnd[gnd], "gnd"

    # 2. Exakter Name
    display = person.get("display_name", "") or person.get("name_from_map", "")
    norm = normalize_name(display)
    if norm and norm in pmb_by_name:
        cands = pmb_by_name[norm]
        if len(cands) == 1:
            if dates_compatible(person, cands[0]):
                return "matched", cands[0], "name_exact"
            return "review", cands[0], "name_exact_dates_mismatch"
        return "review", cands[0], f"name_exact_multiple({len(cands)})"

    # 3. Nachname+Vorname
    surname, forename = extract_surname_forename(display)
    if surname and surname not in ("...", "Unbekannt", "Unbekannte"):
        norm_sn = normalize_name(surname)
        matches = []
        for entries in pmb_by_name.values():
            for e in entries:
                if normalize_name(e.get("surname", "")) == norm_sn:
                    if forename:
                        nf = normalize_name(forename).rstrip(".")
                        ef = normalize_name(e.get("forename", ""))
                        if ef.startswith(nf):
                            matches.append(e)
        if len(matches) == 1:
            return "review", matches[0], "surname_forename_match"

    return "not_found", None, ""


def match_org(org, pmb_by_gnd, pmb_by_name):
    gnd = org.get("gnd")
    if gnd and gnd in pmb_by_gnd:
        return "matched", pmb_by_gnd[gnd], "gnd"

    display = org.get("display_name", "")
    name_clean = re.sub(r"\s*<.*?>", "", display).strip()
    norm = normalize_name(name_clean)
    if norm and norm in pmb_by_name:
        cands = pmb_by_name[norm]
        if len(cands) == 1:
            return "matched", cands[0], "name_exact"
        return "review", cands[0], f"name_exact_multiple({len(cands)})"

    return "not_found", None, ""


# ===================================================================
# XML-Ausgabe
# ===================================================================

def prettify_xml(elem):
    """ElementTree → hübsch formatiertes XML."""
    rough = ET.tostring(elem, encoding="unicode")
    parsed = minidom.parseString(rough)
    lines = parsed.toprettyxml(indent="  ", encoding=None).split("\n")
    # XML-Deklaration entfernen (wir setzen unsere eigene)
    return "\n".join(lines[1:])


def write_listperson_xml(path, person_map):
    """Schreibe data/indices/listperson.xml mit PMB-Verweisen."""
    root = ET.Element("TEI", xmlns=TEI_NS)
    header = ET.SubElement(root, "teiHeader")
    fd = ET.SubElement(header, "fileDesc")
    ts = ET.SubElement(fd, "titleStmt")
    ET.SubElement(ts, "title").text = (
        "Personenverzeichnis der Korrespondenz Arthur Schnitzlers")
    ps = ET.SubElement(fd, "publicationStmt")
    ET.SubElement(ps, "p").text = "Automatisch generiert"
    sd = ET.SubElement(fd, "sourceDesc")
    ET.SubElement(sd, "p").text = "PMB-Abgleich"
    rd = ET.SubElement(header, "revisionDesc")
    ch = ET.SubElement(rd, "change")
    ch.set("when-iso", date.today().isoformat())
    ch.text = "generated by pmb_abgleich.py"

    text = ET.SubElement(root, "text")
    body = ET.SubElement(text, "body")
    lp = ET.SubElement(body, "listPerson")

    for pid in sorted(person_map, key=lambda k: person_map[k].get(
            "display_name", "")):
        info = person_map[pid]
        pmb_id = info.get("pmb_id", "")

        person_el = ET.SubElement(lp, "person")
        if pmb_id:
            person_el.set("xml:id", f"pmb{pmb_id}")
        else:
            person_el.set("xml:id", pid.replace(" ", "_"))

        pn = ET.SubElement(person_el, "persName")
        surname, forename = extract_surname_forename(info["display_name"])
        if forename:
            ET.SubElement(pn, "forename").text = forename
        ET.SubElement(pn, "surname").text = surname

        birth = info.get("birth", "")
        death = info.get("death", "")
        if birth:
            b_el = ET.SubElement(person_el, "birth")
            ET.SubElement(b_el, "date").set("when-iso", birth)
        if death:
            d_el = ET.SubElement(person_el, "death")
            ET.SubElement(d_el, "date").set("when-iso", death)

        # PMB-Link
        if pmb_id:
            idno = ET.SubElement(person_el, "idno")
            idno.set("type", "URL")
            idno.set("subtype", "pmb")
            idno.text = f"https://pmb.acdh.oeaw.ac.at/entity/{pmb_id}/"

        # GND-Link
        gnd = info.get("gnd", "")
        if gnd:
            idno = ET.SubElement(person_el, "idno")
            idno.set("type", "URL")
            idno.set("subtype", "gnd")
            idno.text = f"https://d-nb.info/gnd/{gnd}"

        # DLA-Link
        dla_id = info.get("dla_id", "")
        if dla_id and dla_id.startswith("PE"):
            idno = ET.SubElement(person_el, "idno")
            idno.set("type", "URL")
            idno.set("subtype", "dla")
            idno.text = (
                f"https://www.dla-marbach.de/find/opac/id/{dla_id}")

        # Quell-Info
        source = info.get("source", "")
        if source:
            note = ET.SubElement(person_el, "note")
            note.set("type", "source")
            note.text = source

    xml_str = '<?xml version="1.0" encoding="UTF-8"?>\n' + prettify_xml(root)
    Path(path).write_text(xml_str, encoding="utf-8")


def write_listorg_xml(path, org_map):
    """Schreibe data/indices/listorg.xml mit PMB-Verweisen."""
    root = ET.Element("TEI", xmlns=TEI_NS)
    header = ET.SubElement(root, "teiHeader")
    fd = ET.SubElement(header, "fileDesc")
    ts = ET.SubElement(fd, "titleStmt")
    ET.SubElement(ts, "title").text = (
        "Organisationsverzeichnis der Korrespondenz Arthur Schnitzlers")
    ps = ET.SubElement(fd, "publicationStmt")
    ET.SubElement(ps, "p").text = "Automatisch generiert"
    sd = ET.SubElement(fd, "sourceDesc")
    ET.SubElement(sd, "p").text = "PMB-Abgleich"
    rd = ET.SubElement(header, "revisionDesc")
    ch = ET.SubElement(rd, "change")
    ch.set("when-iso", date.today().isoformat())
    ch.text = "generated by pmb_abgleich.py"

    text = ET.SubElement(root, "text")
    body = ET.SubElement(text, "body")
    lo = ET.SubElement(body, "listOrg")

    for oid in sorted(org_map, key=lambda k: org_map[k].get(
            "display_name", "")):
        info = org_map[oid]
        pmb_id = info.get("pmb_id", "")

        org_el = ET.SubElement(lo, "org")
        if pmb_id:
            org_el.set("xml:id", f"pmb{pmb_id}")
        else:
            safe_id = re.sub(r"[^a-zA-Z0-9_-]", "_", oid)[:80]
            org_el.set("xml:id", f"org_{safe_id}")

        # Org-Name: entferne <Ort>
        name_clean = re.sub(r"\s*<.*?>", "", info["display_name"]).strip()
        ET.SubElement(org_el, "orgName").text = name_clean

        # Ort extrahieren
        m = re.search(r"<(.+?)>", info["display_name"])
        if m:
            loc = ET.SubElement(org_el, "location")
            ET.SubElement(loc, "placeName").text = m.group(1)

        if pmb_id:
            idno = ET.SubElement(org_el, "idno")
            idno.set("type", "URL")
            idno.set("subtype", "pmb")
            idno.text = f"https://pmb.acdh.oeaw.ac.at/entity/{pmb_id}/"

    xml_str = '<?xml version="1.0" encoding="UTF-8"?>\n' + prettify_xml(root)
    Path(path).write_text(xml_str, encoding="utf-8")


def write_briefe_xml(path, briefe, person_map, org_map):
    """Schreibe data/editions/briefe.xml – alle Briefe mit PMB-Refs."""
    root = ET.Element("TEI", xmlns=TEI_NS)
    header = ET.SubElement(root, "teiHeader")
    fd = ET.SubElement(header, "fileDesc")
    ts = ET.SubElement(fd, "titleStmt")
    ET.SubElement(ts, "title").text = (
        "Verzeichnis der Briefe von und an Arthur Schnitzler")
    ps = ET.SubElement(fd, "publicationStmt")
    ET.SubElement(ps, "p").text = "Automatisch generiert"
    sd = ET.SubElement(fd, "sourceDesc")
    ET.SubElement(sd, "p").text = (
        f"{len(briefe)} Briefe aus DLA Marbach und CUL Cambridge")
    rd = ET.SubElement(header, "revisionDesc")
    ch = ET.SubElement(rd, "change")
    ch.set("when-iso", date.today().isoformat())
    ch.text = "generated by pmb_abgleich.py"

    text = ET.SubElement(root, "text")
    body = ET.SubElement(text, "body")
    lb = ET.SubElement(body, "listBibl")

    for brief in sorted(briefe, key=brief_sort_key):
        bs = ET.SubElement(lb, "biblStruct")
        bs.set("xml:id", brief["id"])
        if brief.get("source"):
            bs.set("source", brief["source"])

        an = ET.SubElement(bs, "analytic")
        ET.SubElement(an, "title").text = brief.get("display", "")

        # Sender
        for i, name in enumerate(brief.get("sender_names", [])):
            pid = (brief.get("sender_ids") or [])[i] \
                if i < len(brief.get("sender_ids") or []) else None
            author = ET.SubElement(an, "author")
            pmb_ref = get_pmb_ref(pid, person_map)
            if pmb_ref:
                author.set("ref",
                           f"https://pmb.acdh.oeaw.ac.at/entity/{pmb_ref}/")
            pn = ET.SubElement(author, "persName")
            pn.text = clean_display_name(name)

        # Empfänger
        for i, name in enumerate(brief.get("receiver_names", [])):
            pid = (brief.get("receiver_ids") or [])[i] \
                if i < len(brief.get("receiver_ids") or []) else None
            resp = ET.SubElement(an, "respStmt")
            resp.set("role", "Adressat")
            pmb_ref = get_pmb_ref(pid, person_map)
            if pmb_ref:
                resp.set("ref",
                         f"https://pmb.acdh.oeaw.ac.at/entity/{pmb_ref}/")
            pn = ET.SubElement(resp, "persName")
            pn.text = clean_display_name(name)

        # Datum und Metadaten
        monogr = ET.SubElement(bs, "monogr")
        imprint = ET.SubElement(monogr, "imprint")

        date_iso = brief.get("date_iso", "")
        date_display = brief.get("date_display", "")
        date_range = brief.get("date_range")

        date_el = ET.SubElement(imprint, "date")
        if date_iso:
            date_el.set("when", date_iso)
            date_el.text = date_display or date_iso
        elif date_range and isinstance(date_range, list) and date_range:
            dr = date_range[0]
            if "/" in dr:
                parts = dr.split("/")
                date_el.set("from", parts[0])
                date_el.set("to", parts[1])
                date_el.text = f"{parts[0]}–{parts[1]}"
            elif dr:
                date_el.set("when", dr)
                date_el.text = dr
            else:
                date_el.set("type", "undated")
                date_el.text = "undatiert"
        else:
            date_el.set("type", "undated")
            date_el.text = "undatiert"

        extent = brief.get("extent", "")
        if extent:
            ET.SubElement(monogr, "extent").text = extent

        # IDs
        url = brief.get("url", "")
        if url:
            idno = ET.SubElement(bs, "idno")
            idno.set("type", "url")
            idno.text = url

        accession = brief.get("accession", "")
        if accession:
            idno = ET.SubElement(bs, "idno")
            idno.set("type", "accession")
            idno.text = accession

    xml_str = '<?xml version="1.0" encoding="UTF-8"?>\n' + prettify_xml(root)
    Path(path).write_text(xml_str, encoding="utf-8")


def get_pmb_ref(pid, person_map):
    """PMB-ID für eine DLA-Person-ID holen."""
    if not pid:
        return None
    info = person_map.get(pid)
    if info:
        return info.get("pmb_id")
    return None


def clean_display_name(name):
    """Lebensdaten aus Display-Name entfernen."""
    return re.sub(r"\s*\([\d\-–]+\)", "", name).strip()


def brief_sort_key(brief):
    """Sortierung: nach Datum, dann ID."""
    d = brief.get("date_iso", "") or ""
    return (d or "9999", brief.get("id", ""))


# ===================================================================
# CSV-Ausgabe
# ===================================================================

def write_csv(path, fieldnames, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


# ===================================================================
# Review-Einlesen
# ===================================================================

def load_review_done(path):
    """Lese review_done.csv ein.

    Erwartete Spalte 'decision':
      y  → Match bestätigt, PMB-ID übernehmen
      n  → Match abgelehnt, zum Import schicken
      (leer oder fehlend) → noch offen, bleibt in review

    Optional: Spalte 'pmb_id_corrected' → manuell korrigierte PMB-ID
    (z.B. wenn der automatische Vorschlag falsch war, aber die
    richtige PMB-ID bekannt ist)

    Returns: confirmed dict, rejected list, pending list
      confirmed: source_id → pmb_id
      rejected:  [row, ...]
      pending:   [row, ...]
    """
    confirmed = {}  # source_id → pmb_id
    rejected = []
    pending = []

    if not Path(path).exists():
        return confirmed, rejected, pending

    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            decision = (row.get("decision") or "").strip().lower()
            sid = (row.get("source_id") or "").strip()
            if not sid:
                continue

            if decision == "y":
                # Manuell korrigierte PMB-ID hat Vorrang
                pmb_id = (row.get("pmb_id_corrected") or "").strip()
                if not pmb_id:
                    pmb_id = (row.get("pmb_id") or "").strip()
                if pmb_id:
                    confirmed[sid] = pmb_id
            elif decision == "n":
                rejected.append(row)
            else:
                pending.append(row)

    return confirmed, rejected, pending


# ===================================================================
# Hauptprogramm
# ===================================================================

def main():
    print("Lade PMB-Exportdaten...")
    pmb_p_gnd, pmb_p_name = parse_pmb_persons(
        BASE / "pmb-export" / "listperson.xml")
    pmb_o_gnd, pmb_o_name = parse_pmb_orgs(
        BASE / "pmb-export" / "listorg.xml")
    print(f"  PMB: {len(pmb_p_gnd)} Personen mit GND, "
          f"{len(pmb_o_gnd)} Orgs mit GND")

    print("Lade Quelldaten...")
    dla_persons, dla_orgs, dla_briefe = load_dla(
        BASE / "schnitzler_briefe_raw.json",
        BASE / "person_gnd_map.json")
    cul_persons, cul_briefe = load_cul(
        BASE / "CUL Handlist B - Tabellenblatt1.csv")
    print(f"  DLA: {len(dla_persons)} Personen, {len(dla_orgs)} Orgs, "
          f"{len(dla_briefe)} Briefe")
    print(f"  CUL: {len(cul_persons)} Personen, {len(cul_briefe)} Briefe")

    # --- review_done.csv einlesen ---
    review_done_path = BASE / "output" / "review_done.csv"
    confirmed, rejected, pending_review = load_review_done(review_done_path)
    if confirmed or rejected or pending_review:
        print(f"\nreview_done.csv geladen:")
        print(f"  bestätigt (y): {len(confirmed)}")
        print(f"  abgelehnt (n): {len(rejected)}")
        print(f"  offen:         {len(pending_review)}")

    # --- Matching ---
    review_rows = []
    import_rows = []
    stats = defaultdict(lambda: defaultdict(int))

    print("\nMatche Personen...")
    all_persons = dict(dla_persons)
    for key, info in cul_persons.items():
        if info.get("pmb_id"):
            info["match_type"] = "cul_known"
            stats["person"]["matched"] += 1
            if key not in all_persons:
                all_persons[key] = info
            continue
        if key not in all_persons:
            all_persons[key] = info

    for pid, info in sorted(all_persons.items()):
        if info.get("pmb_id"):
            continue

        # Bestätigte Reviews direkt übernehmen
        if pid in confirmed:
            info["pmb_id"] = confirmed[pid]
            info["match_type"] = "review_confirmed"
            stats["person"]["matched"] += 1
            continue

        status, pmb_entry, confidence = match_person(
            info, pmb_p_gnd, pmb_p_name)

        row = dict(
            source=info.get("source", "DLA"),
            source_id=pid,
            display_name=info["display_name"],
            gnd=info.get("gnd") or "",
            birth=info.get("birth") or "",
            death=info.get("death") or "",
            entity_type="person")

        if status == "matched":
            info["pmb_id"] = pmb_entry["pmb_id"]
            info["match_type"] = confidence
            stats["person"]["matched"] += 1
        elif status == "review":
            # Wenn schon abgelehnt → direkt zum Import
            if pid in [r.get("source_id") for r in rejected]:
                surname, forename = extract_surname_forename(
                    info["display_name"])
                row["surname"] = surname
                row["forename"] = forename
                row["gnd_url"] = (
                    f"https://d-nb.info/gnd/{info['gnd']}"
                    if info.get("gnd") else "")
                import_rows.append(row)
                stats["person"]["import"] += 1
            else:
                info["pmb_id"] = pmb_entry["pmb_id"]
                info["match_type"] = confidence
                row["pmb_id"] = pmb_entry["pmb_id"]
                row["pmb_name"] = pmb_entry.get("full_name", "")
                row["pmb_url"] = (
                    f"https://pmb.acdh.oeaw.ac.at/entity/"
                    f"{pmb_entry['pmb_id']}/")
                row["match_type"] = confidence
                review_rows.append(row)
                stats["person"]["review"] += 1
        else:
            surname, forename = extract_surname_forename(
                info["display_name"])
            row["surname"] = surname
            row["forename"] = forename
            row["gnd_url"] = (
                f"https://d-nb.info/gnd/{info['gnd']}"
                if info.get("gnd") else "")
            import_rows.append(row)
            stats["person"]["import"] += 1

    print("Matche Organisationen...")
    for oid, info in sorted(dla_orgs.items()):
        # Bestätigte Reviews
        if oid in confirmed:
            info["pmb_id"] = confirmed[oid]
            stats["org"]["matched"] += 1
            continue

        status, pmb_entry, confidence = match_org(
            info, pmb_o_gnd, pmb_o_name)

        row = dict(
            source="DLA", source_id=oid,
            display_name=info["display_name"],
            gnd="", birth="", death="",
            entity_type="org")

        if status == "matched":
            info["pmb_id"] = pmb_entry["pmb_id"]
            stats["org"]["matched"] += 1
        elif status == "review":
            if oid in [r.get("source_id") for r in rejected]:
                name_clean = re.sub(
                    r"\s*<.*?>", "", info["display_name"])
                m = re.search(r"<(.+?)>", info["display_name"])
                row["surname"] = name_clean.strip()
                row["forename"] = m.group(1) if m else ""
                import_rows.append(row)
                stats["org"]["import"] += 1
            else:
                info["pmb_id"] = pmb_entry["pmb_id"]
                row["pmb_id"] = pmb_entry["pmb_id"]
                row["pmb_name"] = pmb_entry.get("name", "")
                row["pmb_url"] = (
                    f"https://pmb.acdh.oeaw.ac.at/entity/"
                    f"{pmb_entry['pmb_id']}/")
                row["match_type"] = confidence
                review_rows.append(row)
                stats["org"]["review"] += 1
        else:
            name_clean = re.sub(r"\s*<.*?>", "", info["display_name"])
            m = re.search(r"<(.+?)>", info["display_name"])
            row["surname"] = name_clean.strip()
            row["forename"] = m.group(1) if m else ""
            import_rows.append(row)
            stats["org"]["import"] += 1

    # --- Ausgabe ---
    indices_dir = BASE / "data" / "indices"
    editions_dir = BASE / "data" / "editions"
    output_dir = BASE / "output"
    indices_dir.mkdir(parents=True, exist_ok=True)
    editions_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("\nSchreibe data/indices/listperson.xml...")
    write_listperson_xml(indices_dir / "listperson.xml", all_persons)

    print("Schreibe data/indices/listorg.xml...")
    write_listorg_xml(indices_dir / "listorg.xml", dla_orgs)

    print("Schreibe data/editions/briefe.xml...")
    all_briefe = dla_briefe + cul_briefe
    write_briefe_xml(
        editions_dir / "briefe.xml", all_briefe, all_persons, dla_orgs)

    # CSVs
    review_fields = [
        "source", "source_id", "display_name", "gnd", "birth", "death",
        "entity_type", "pmb_id", "pmb_name", "pmb_url", "match_type",
        "decision", "pmb_id_corrected"]
    write_csv(output_dir / "review.csv", review_fields, review_rows)

    import_fields = [
        "entity_type", "surname", "forename", "display_name",
        "gnd", "gnd_url", "birth", "death", "source", "source_id"]
    write_csv(output_dir / "import_to_pmb.csv", import_fields, import_rows)

    # --- Zusammenfassung ---
    pm = stats["person"]["matched"]
    pr = stats["person"]["review"]
    pi = stats["person"]["import"]
    om = stats["org"]["matched"]
    orr = stats["org"]["review"]
    oi = stats["org"]["import"]

    print(f"\n{'='*60}")
    print("ERGEBNIS")
    print(f"{'='*60}")
    print(f"{'':20} {'matched':>10} {'review':>10} {'import':>10}")
    print(f"{'Personen':<20} {pm:>10} {pr:>10} {pi:>10}")
    print(f"{'Organisationen':<20} {om:>10} {orr:>10} {oi:>10}")
    print(f"{'─'*60}")
    print(f"{'Gesamt':<20} {pm+om:>10} {pr+orr:>10} {pi+oi:>10}")
    print(f"\ndata/indices/listperson.xml  ({len(all_persons)} Personen)")
    print(f"data/indices/listorg.xml     ({len(dla_orgs)} Organisationen)")
    print(f"data/editions/briefe.xml     ({len(all_briefe)} Briefe)")
    print(f"output/review.csv            ({len(review_rows)} → prüfen)")
    print(f"output/import_to_pmb.csv     ({len(import_rows)} → GSheet)")
    if confirmed:
        print(f"\n  {len(confirmed)} Einträge aus review_done.csv übernommen")


if __name__ == "__main__":
    main()

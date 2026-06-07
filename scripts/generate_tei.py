#!/usr/bin/env python3
"""
Erzeugt TEI-XML-Dateien aus den JSON-Import-Daten.

Verarbeitet:
  - json-import/cul_briefe_an_schnitzler.json    (Briefe an S., CUL)
  - json-import/cul_schnitzler_als_autor.json     (Manuskripte/Dokumente/Briefe von, CUL)
  - json-import/dla_briefe_an_schnitzler.json     (Briefe an S., DLA)
  - json-import/dla_schnitzler_als_autor.json     (Manuskripte/Dokumente/Briefe von, DLA)

Liest:
  - pmb-export/listperson.xml  (PMB-Personen für Referenz-IDs)
  - pmb-export/listplace.xml   (PMB-Orte für Referenz-IDs)

Schreibt:
  - data/editions/{id}.xml     (eine TEI-Datei pro Eintrag)
"""

import json
import re
import xml.etree.ElementTree as ET
from datetime import date
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
NS = {"tei": "http://www.tei-c.org/ns/1.0"}

TODAY = date.today().isoformat()

JSON_FILES = [
    "json-import/cul_briefe_an_schnitzler.json",
    "json-import/cul_schnitzler_als_autor.json",
    "json-import/dla_briefe_an_schnitzler.json",
    "json-import/dla_schnitzler_als_autor.json",
]


# ───────────────────────────────────────────────────
# PMB-Lookups aufbauen
# ───────────────────────────────────────────────────

def build_person_lookup(path):
    """PMB listperson.xml → (by_display, by_name, by_dla_id)."""
    tree = ET.parse(path)
    by_display = {}
    by_name = {}
    by_dla_id = {}

    for p in tree.findall(".//tei:person", NS):
        xml_id = p.get("{http://www.w3.org/XML/1998/namespace}id", "")
        pmb_id = xml_id.replace("person__", "")
        if not pmb_id:
            continue

        forename = surname = ""
        for pn in p.findall("tei:persName", NS):
            if not pn.get("type"):
                forename = (pn.findtext("tei:forename", "", NS) or "").strip()
                surname = (pn.findtext("tei:surname", "", NS) or "").strip()
                break

        birth = death = ""
        b_el = p.find("tei:birth/tei:date", NS)
        if b_el is not None:
            birth = b_el.get("when-iso", "")[:4]
        d_el = p.find("tei:death/tei:date", NS)
        if d_el is not None:
            death = d_el.get("when-iso", "")[:4]

        simple = f"{surname}, {forename}".strip(", ")
        display = f"{surname}, {forename} ({birth}-{death})" if (birth or death) else simple

        by_display[display] = pmb_id
        if simple not in by_name:
            by_name[simple] = pmb_id

        for idno in p.findall("tei:idno", NS):
            if idno.get("subtype") == "dla-marbach":
                url = (idno.text or "").strip()
                if "/id/" in url:
                    dla_id = url.split("/id/")[-1].strip("/")
                    if dla_id:
                        by_dla_id[dla_id] = pmb_id

    return by_display, by_name, by_dla_id


def build_place_lookup(path):
    """PMB listplace.xml → Ortsname (lowercase) → pmb_id."""
    tree = ET.parse(path)
    places = {}
    for p in tree.findall(".//tei:place", NS):
        xml_id = p.get("{http://www.w3.org/XML/1998/namespace}id", "")
        pmb_id = xml_id.replace("place__", "")
        name = (p.findtext("tei:placeName", "", NS) or "").strip()
        if name:
            places[name.lower()] = pmb_id
    return places


def resolve_person(display_name, by_display, by_name, by_dla_id=None, dla_id=None):
    """Versucht eine PMB-ID für einen DLA-Anzeigenamen / DLA-ID zu finden."""
    if dla_id and by_dla_id and dla_id in by_dla_id:
        return by_dla_id[dla_id]
    if display_name in by_display:
        return by_display[display_name]
    clean = re.sub(r"\s*\(.*?\)\s*", "", display_name).strip()
    if clean in by_name:
        return by_name[clean]
    return None


def build_org_lookup(path):
    """PMB listorg.xml → (by_name, by_dla_id)."""
    tree = ET.parse(path)
    by_name = {}
    by_dla_id = {}
    for o in tree.findall(".//tei:org", NS):
        xml_id = o.get("{http://www.w3.org/XML/1998/namespace}id", "")
        pmb_id = xml_id.replace("org__", "")
        name = (o.findtext("tei:orgName", "", NS) or "").strip()
        if name and name not in by_name:
            by_name[name] = pmb_id
        for idno in o.findall("tei:idno", NS):
            if idno.get("subtype") == "dla-marbach":
                url = (idno.text or "").strip()
                if "/id/" in url:
                    dla_id = url.split("/id/")[-1].strip("/")
                    if dla_id:
                        by_dla_id[dla_id] = pmb_id
    return by_name, by_dla_id


def resolve_org(display_name, org_lookup, org_by_dla_id=None, dla_id=None):
    """Versucht eine PMB-ID für eine Organisation zu finden."""
    if dla_id and org_by_dla_id and dla_id in org_by_dla_id:
        return org_by_dla_id[dla_id]
    if display_name in org_lookup:
        return org_lookup[display_name]
    clean = re.sub(r"\s*\(.*?\)\s*$", "", display_name).strip()
    if clean in org_lookup:
        return org_lookup[clean]
    # Versuch ohne spitze Klammern
    clean2 = re.sub(r"\s*<.*?>\s*", "", clean).strip()
    if clean2 in org_lookup:
        return org_lookup[clean2]
    return None


def resolve_place(place_name, place_lookup):
    """PMB-ID für einen Ortsnamen finden."""
    return place_lookup.get(place_name.lower())


def extract_name_parts(display_name):
    """'Adam, Robert (1877-1961)' → ('Adam', 'Robert')"""
    clean = re.sub(r"\s*\(.*?\)\s*", "", display_name).strip()
    parts = clean.split(",", 1)
    surname = parts[0].strip()
    forename = parts[1].strip() if len(parts) > 1 else ""
    return surname, forename


# ───────────────────────────────────────────────────
# XML-Hilfsfunktionen
# ───────────────────────────────────────────────────

def xml_escape(text):
    return (text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;"))


def make_person_ref(display_name, pmb_id):
    surname, forename = extract_name_parts(display_name)
    full = f"{surname}, {forename}" if forename else surname
    if pmb_id:
        return f'<persName ref="#pmb{pmb_id}">{xml_escape(full)}</persName>'
    return f"<persName>{xml_escape(full)}</persName>"


def extract_org_name(display_name):
    """'Neue Rundschau <Zeitschrift, Berlin [u.a.]>' → 'Neue Rundschau'"""
    clean = re.sub(r"\s*\(.*?\)\s*$", "", display_name).strip()
    clean = re.sub(r"\s*<.*?>\s*", "", clean).strip()
    return clean


def make_org_ref(display_name, pmb_id):
    name = extract_org_name(display_name)
    if pmb_id:
        return f'<orgName ref="#pmb{pmb_id}">{xml_escape(name)}</orgName>'
    return f"<orgName>{xml_escape(name)}</orgName>"


def make_place_ref(place_name, pmb_id):
    if pmb_id:
        return f'<placeName ref="#pmb{pmb_id}">{xml_escape(place_name)}</placeName>'
    return f"<placeName>{xml_escape(place_name)}</placeName>"


# ───────────────────────────────────────────────────
# Datums-Parsing
# ───────────────────────────────────────────────────

def parse_dates(entry):
    """Gibt (date_start, date_end) zurück, bereinigt."""
    ds = (entry.get("dateOriginStart") or "").strip().rstrip("-")
    de = (entry.get("dateOriginEnd") or "").strip().rstrip("-")
    return ds, de


def date_iso_attr(date_start, date_end):
    """Erzeugt das passende ISO-Datums-Attribut für title."""
    if date_start and date_end and date_start != date_end:
        return f' notBefore="{date_start}" notAfter="{date_end}"'
    elif date_start:
        return f' when-iso="{date_start}"'
    return ""


# ───────────────────────────────────────────────────
# msIdentifier je nach Standort
# ───────────────────────────────────────────────────

def build_ms_identifier(entry):
    standort = entry.get("standort", "")
    if standort == "CUL":
        sig = entry.get("cul_signatur", entry.get("accessionNumber", ""))
        return (
            "                        <country>GB</country>\n"
            "                        <settlement>Cambridge</settlement>\n"
            "                        <repository>Cambridge University Library</repository>\n"
            f"                        <idno>{xml_escape(sig)}</idno>")
    else:
        sig = entry.get("accessionNumber", "")
        return (
            "                        <country>D</country>\n"
            "                        <settlement>Marbach am Neckar</settlement>\n"
            "                        <repository>Deutsches Literaturarchiv</repository>\n"
            f"                        <idno>{xml_escape(sig)}</idno>")


# ───────────────────────────────────────────────────
# correspDesc für Briefe
# ───────────────────────────────────────────────────

def build_corresp_action(action_type, persons, person_ids, orgs, org_ids, places,
                         by_display, by_name, person_by_dla_id,
                         org_lookup, org_by_dla_id,
                         place_lookup, date_start=None, date_end=None):
    lines = [f'            <correspAction type="{action_type}">']

    for i, p in enumerate(persons):
        dla_id = person_ids[i] if i < len(person_ids) else None
        pmb_id = resolve_person(p, by_display, by_name, person_by_dla_id, dla_id)
        lines.append(f"               {make_person_ref(p, pmb_id)}")

    for i, o in enumerate(orgs):
        dla_id = org_ids[i] if i < len(org_ids) else None
        pmb_id = resolve_org(o, org_lookup, org_by_dla_id, dla_id)
        lines.append(f"               {make_org_ref(o, pmb_id)}")

    if date_start and date_end and date_start != date_end:
        lines.append(f'               <date notBefore="{xml_escape(date_start)}" notAfter="{xml_escape(date_end)}">'
                      f'{xml_escape(date_start)}/{xml_escape(date_end)}</date>')
    elif date_start:
        lines.append(f'               <date when="{xml_escape(date_start)}">{xml_escape(date_start)}</date>')

    for pl in places:
        if pl.lower() == "ohne ort":
            continue
        pmb_id = resolve_place(pl, place_lookup)
        lines.append(f"               {make_place_ref(pl, pmb_id)}")

    lines.append("            </correspAction>")
    return "\n".join(lines)


def build_corresp_desc(entry, by_display, by_name, person_by_dla_id,
                       org_lookup, org_by_dla_id, place_lookup):
    """Baut das gesamte correspDesc-Element für einen Brief."""
    date_start, date_end = parse_dates(entry)
    senders = entry.get("personBy_display_mv", [])
    sender_ids = entry.get("personBy_id_mv", [])
    receivers = entry.get("personTo_display_mv", [])
    receiver_ids = entry.get("personTo_id_mv", [])
    sender_orgs = entry.get("corporationBy_display_mv", [])
    sender_org_ids = entry.get("corporationBy_id_mv", [])
    receiver_orgs = entry.get("corporationTo_display_mv", [])
    receiver_org_ids = entry.get("corporationTo_id_mv", [])
    places = entry.get("place_mv", [])

    corresp_sent = build_corresp_action(
        "sent", senders, sender_ids, sender_orgs, sender_org_ids, places,
        by_display, by_name, person_by_dla_id,
        org_lookup, org_by_dla_id, place_lookup, date_start, date_end)

    corresp_received = build_corresp_action(
        "received", receivers, receiver_ids, receiver_orgs, receiver_org_ids, [],
        by_display, by_name, person_by_dla_id,
        org_lookup, org_by_dla_id, place_lookup)

    # correspContext: belongsToCorrespondence (Absender-PMB-ID)
    corresp_context = ""
    for i, p in enumerate(senders):
        dla_id = sender_ids[i] if i < len(sender_ids) else None
        pmb_id = resolve_person(p, by_display, by_name, person_by_dla_id, dla_id)
        if pmb_id:
            surname, forename = extract_name_parts(p)
            full = f"{surname}, {forename}" if forename else surname
            corresp_context = (
                f'            <correspContext>\n'
                f'               <ref type="belongsToCorrespondence" target="correspondence_{pmb_id}">'
                f'{xml_escape(full)}</ref>\n'
                f'            </correspContext>')
            break

    return (
        f"         <correspDesc>\n"
        f"{corresp_sent}\n"
        f"{corresp_received}\n"
        f"{corresp_context}\n"
        f"         </correspDesc>")


# ───────────────────────────────────────────────────
# Kategorie-Erkennung
# ───────────────────────────────────────────────────

def entry_type(entry):
    """Gibt 'brief', 'manuskript' oder 'dokument' zurück."""
    cat = entry.get("category", "")
    if cat in ("Briefe an", "Briefe von", "Briefe anderer"):
        return "brief"
    elif cat == "Manuskripte":
        return "manuskript"
    else:
        return "dokument"


def objecttype_corresp(entry):
    """objectType-Wert für das TEI-Element."""
    t = entry_type(entry)
    if t == "brief":
        return "brief"
    elif t == "manuskript":
        return "manuskript"
    else:
        return "dokument"


# ───────────────────────────────────────────────────
# TEI-Erzeugung
# ───────────────────────────────────────────────────

def build_tei(entry, by_display, by_name, person_by_dla_id,
              org_lookup, org_by_dla_id, place_lookup):
    eid = entry["id"]
    title_text = xml_escape(entry.get("title", eid))
    date_start, date_end = parse_dates(entry)
    date_display = entry.get("displayAddition1", "")
    dla_url = entry.get("dla_url", entry.get("url", ""))
    extent = entry.get("extent", "")
    etype = entry_type(entry)

    senders = entry.get("personBy_display_mv", [])
    sender_ids = entry.get("personBy_id_mv", [])

    # title level="s"
    title_s = "Arthur Schnitzler: Autografen"

    # title level="a" – spitze Klammern entfernen
    raw_title = re.sub(r"\s*<[^>]*>", "", entry.get("title", ""))
    # Bei Briefen Datum bzw. Zeitspanne an den Titel anhängen
    if etype == "brief":
        date_for_title = ""
        if date_display:
            # " - " → "–" (Gedankenstrich ohne Leerzeichen)
            date_for_title = re.sub(r"\s*-\s*", "–", date_display)
        elif date_start and date_end and date_start != date_end:
            date_for_title = f"{date_start}–{date_end}"
        elif date_start:
            date_for_title = date_start
        if date_for_title:
            # Führende Nullen in Tag/Monat entfernen (04.12.1915 → 4.12.1915)
            date_for_title = re.sub(r"\b0(\d)", r"\1", date_for_title)
            raw_title = f"{raw_title}, {date_for_title}"
    title_a = xml_escape(raw_title)

    # ISO-Datums-Attribut
    dattr = date_iso_attr(date_start, date_end)

    # Datum-Anzeige
    date_text = xml_escape(date_display) if date_display else xml_escape(date_start)

    # author-Elemente
    author_lines = []
    if etype == "brief":
        for i, s in enumerate(senders):
            dla_id = sender_ids[i] if i < len(sender_ids) else None
            pmb_id = resolve_person(s, by_display, by_name, person_by_dla_id, dla_id)
            surname, forename = extract_name_parts(s)
            full = f"{surname}, {forename}" if forename else surname
            ref = f' ref="#pmb{pmb_id}"' if pmb_id else ""
            author_lines.append(f'            <author{ref}>{xml_escape(full)}</author>')
    else:
        # Manuskripte/Dokumente: Schnitzler ist immer Autor
        for i, s in enumerate(senders):
            dla_id = sender_ids[i] if i < len(sender_ids) else None
            pmb_id = resolve_person(s, by_display, by_name, person_by_dla_id, dla_id)
            surname, forename = extract_name_parts(s)
            full = f"{surname}, {forename}" if forename else surname
            ref = f' ref="#pmb{pmb_id}"' if pmb_id else ""
            author_lines.append(f'            <author{ref}>{xml_escape(full)}</author>')
        if not author_lines:
            author_lines.append('            <author ref="#pmb2121">Schnitzler, Arthur</author>')

    authors_str = "\n".join(author_lines)

    # msIdentifier
    ms_identifier = build_ms_identifier(entry)

    # objectType
    obj_type = objecttype_corresp(entry)

    # profileDesc-Inhalt: correspDesc nur bei Briefen
    if etype == "brief":
        profile_content = (
            f"         <langUsage>\n"
            f'            <language ident="de-AT">German</language>\n'
            f"         </langUsage>\n"
            f"{build_corresp_desc(entry, by_display, by_name, person_by_dla_id, org_lookup, org_by_dla_id, place_lookup)}")
    else:
        profile_content = (
            f"         <langUsage>\n"
            f'            <language ident="de-AT">German</language>\n'
            f"         </langUsage>")

    # Werktitel für Manuskripte/Dokumente
    title_main = entry.get("titleMain_text", "")
    genre = entry.get("genre", "")
    note_section = ""
    if etype in ("manuskript", "dokument") and title_main:
        note_section = (
            f"\n"
            f"      <notesStmt>\n"
            f'         <note type="work_title">{xml_escape(title_main)}</note>\n')
        if genre:
            note_section += f'         <note type="genre">{xml_escape(genre)}</note>\n'
        # Werkbezug
        for w in entry.get("work_display_mv", []):
            note_section += f'         <note type="work_reference">{xml_escape(w)}</note>\n'
        note_section += f"      </notesStmt>"

    tei = f"""<?xml version="1.0" encoding="UTF-8"?>
<TEI xmlns="http://www.tei-c.org/ns/1.0"
     xml:id="{eid}">
   <teiHeader>
      <fileDesc>
         <titleStmt>
            <title level="s">{xml_escape(title_s)}</title>
            <title type="iso-date"{dattr}>{date_text}</title>
            <title level="a">{title_a}</title>
{authors_str}
            <editor>
               <name>Müller, Martin Anton</name>
               <name>Jahnke, Selma</name>
            </editor>
            <funder>
               <name>Österreichischer Wissenschaftsfonds FWF</name>
               <address>
                  <street>Georg-Coch-Platz 2</street>
                  <postCode>1010 Wien</postCode>
                  <placeName>
                     <country>A</country>
                     <settlement>Wien</settlement>
                  </placeName>
               </address>
            </funder>
         </titleStmt>
         <editionStmt>
            <edition>schnitzler-autografen</edition>
            <respStmt>
               <resp>Ersteller</resp>
               <persName>Müller, Martin Anton</persName>
               <persName>Jahnke, Selma</persName>
            </respStmt>
         </editionStmt>
         <publicationStmt>
            <publisher>Austrian Centre for Digital Humanities</publisher>
            <pubPlace>Vienna</pubPlace>
            <date when="2026">2026</date>
            <availability>
               <licence target="https://creativecommons.org/licenses/by/4.0/deed.de">
                  <p>Sie dürfen: Teilen – das Material in jedwedem Format oder Medium
                     vervielfältigen und weiterverbreiten Bearbeiten – das Material remixen,
                     verändern und darauf aufbauen und zwar für beliebige Zwecke, sogar
                     kommerziell.</p>
                  <p>Der Lizenzgeber kann diese Freiheiten nicht widerrufen solange Sie sich an die
                     Lizenzbedingungen halten. Unter folgenden Bedingungen:</p>
                  <p>Namensnennung – Sie müssen angemessene Urheber- und Rechteangaben machen, einen
                     Link zur Lizenz beifügen und angeben, ob Änderungen vorgenommen wurden. Diese
                     Angaben dürfen in jeder angemessenen Art und Weise gemacht werden, allerdings
                     nicht so, dass der Eindruck entsteht, der Lizenzgeber unterstütze gerade Sie
                     oder Ihre Nutzung besonders. Keine weiteren Einschränkungen – Sie dürfen keine
                     zusätzlichen Klauseln oder technische Verfahren einsetzen, die anderen
                     rechtlich irgendetwas untersagen, was die Lizenz erlaubt.</p>
                  <p>Hinweise:</p>
                  <p>Sie müssen sich nicht an diese Lizenz halten hinsichtlich solcher Teile des
                     Materials, die gemeinfrei sind, oder soweit Ihre Nutzungshandlungen durch
                     Ausnahmen und Schranken des Urheberrechts gedeckt sind. Es werden keine
                     Garantien gegeben und auch keine Gewähr geleistet. Die Lizenz verschafft Ihnen
                     möglicherweise nicht alle Erlaubnisse, die Sie für die jeweilige Nutzung
                     brauchen. Es können beispielsweise andere Rechte wie Persönlichkeits- und
                     Datenschutzrechte zu beachten sein, die Ihre Nutzung des Materials entsprechend
                     beschränken.</p>
               </licence>
            </availability>
         </publicationStmt>
         <seriesStmt>
            <p>Machine-Readable Transcriptions of the Correspondences of Arthur Schnitzler</p>
         </seriesStmt>
         <sourceDesc>
            <listWit>
               <witness n="1">
                  <objectType corresp="{obj_type}"/>
                  <msDesc>
                     <msIdentifier>
{ms_identifier}
                     </msIdentifier>
                     <physDesc>
                        <objectDesc>
                           <supportDesc>
                              <extent>
                                 <measure unit="umfang">{xml_escape(extent)}</measure>
                              </extent>
                           </supportDesc>
                        </objectDesc>
                     </physDesc>
                  </msDesc>
               </witness>
            </listWit>
         </sourceDesc>{note_section}
      </fileDesc>
      <profileDesc>
{profile_content}
      </profileDesc>
      <revisionDesc status="proposed">
         <change who="MAM" when="{TODAY}">Angelegt aus {xml_escape(entry.get('standort', 'DLA'))}-Daten</change>
      </revisionDesc>
   </teiHeader>
   <facsimile>
      <surface>
         <graphic url="{xml_escape(dla_url)}"/>
      </surface>
   </facsimile>
   <text>
      <body>
         <div type="writingSession" n="1">
            <p>
               <hi rend="pre-print">wird automatisch befüllt</hi>
            </p>
         </div>
      </body>
   </text>
</TEI>"""
    return tei


# ───────────────────────────────────────────────────
# Main
# ───────────────────────────────────────────────────

def main():
    print("Lade PMB-Personendaten …")
    by_display, by_name, person_by_dla_id = build_person_lookup(BASE / "pmb-export" / "listperson.xml")
    print(f"  {len(by_display)} Anzeigenamen, {len(by_name)} einfache Namen, {len(person_by_dla_id)} DLA-IDs")

    print("Lade PMB-Organisationsdaten …")
    org_lookup, org_by_dla_id = build_org_lookup(BASE / "pmb-export" / "listorg.xml")
    print(f"  {len(org_lookup)} Organisationen, {len(org_by_dla_id)} DLA-IDs")

    print("Lade PMB-Ortsdaten …")
    place_lookup = build_place_lookup(BASE / "pmb-export" / "listplace.xml")
    print(f"  {len(place_lookup)} Orte")

    out_dir = BASE / "data" / "editions"
    out_dir.mkdir(parents=True, exist_ok=True)

    total_written = 0
    total_persons_ok = 0
    total_persons_fail = 0
    total_orgs_ok = 0
    total_orgs_fail = 0
    total_places_ok = 0
    total_places_fail = 0
    all_unresolved_persons = set()
    all_unresolved_orgs = set()
    all_unresolved_places = set()

    for json_file in JSON_FILES:
        json_path = BASE / json_file
        if not json_path.exists():
            print(f"\n⚠ {json_file} nicht gefunden, überspringe.")
            continue

        print(f"\nVerarbeite {json_file} …")
        data = json.loads(json_path.read_text("utf-8"))
        print(f"  {len(data)} Einträge")

        from collections import Counter
        type_counts = Counter()

        for entry in data:
            eid = entry["id"]
            etype = entry_type(entry)
            type_counts[etype] += 1

            tei_xml = build_tei(entry, by_display, by_name, person_by_dla_id,
                                org_lookup, org_by_dla_id, place_lookup)
            standort = entry.get("standort", "").upper()
            out_path = out_dir / f"{standort}_{eid}.xml"
            out_path.write_text(tei_xml, encoding="utf-8")
            total_written += 1

            # Statistik
            for disp_field, id_field in (
                ("personBy_display_mv", "personBy_id_mv"),
                ("personTo_display_mv", "personTo_id_mv"),
            ):
                disps = entry.get(disp_field, [])
                ids = entry.get(id_field, [])
                for i, p in enumerate(disps):
                    dla_id = ids[i] if i < len(ids) else None
                    if resolve_person(p, by_display, by_name, person_by_dla_id, dla_id):
                        total_persons_ok += 1
                    else:
                        total_persons_fail += 1
                        all_unresolved_persons.add(p)

            for disp_field, id_field in (
                ("corporationBy_display_mv", "corporationBy_id_mv"),
                ("corporationTo_display_mv", "corporationTo_id_mv"),
            ):
                disps = entry.get(disp_field, [])
                ids = entry.get(id_field, [])
                for i, o in enumerate(disps):
                    dla_id = ids[i] if i < len(ids) else None
                    if resolve_org(o, org_lookup, org_by_dla_id, dla_id):
                        total_orgs_ok += 1
                    else:
                        total_orgs_fail += 1
                        all_unresolved_orgs.add(o)

            for pl in entry.get("place_mv", []):
                if pl.lower() == "ohne ort":
                    continue
                if resolve_place(pl, place_lookup):
                    total_places_ok += 1
                else:
                    total_places_fail += 1
                    all_unresolved_places.add(pl)

        for t, c in sorted(type_counts.items()):
            print(f"    {t}: {c}")

    print(f"\n{'='*50}")
    print(f"Gesamt: {total_written} TEI-Dateien geschrieben nach {out_dir}")
    print(f"Personen:       {total_persons_ok} aufgelöst, {total_persons_fail} nicht aufgelöst")
    print(f"Organisationen: {total_orgs_ok} aufgelöst, {total_orgs_fail} nicht aufgelöst")
    print(f"Orte:           {total_places_ok} aufgelöst, {total_places_fail} nicht aufgelöst")

    if all_unresolved_persons:
        print(f"\nNicht aufgelöste Personen ({len(all_unresolved_persons)}):")
        for p in sorted(all_unresolved_persons):
            print(f"  {p}")

    if all_unresolved_orgs:
        print(f"\nNicht aufgelöste Organisationen ({len(all_unresolved_orgs)}):")
        for o in sorted(all_unresolved_orgs):
            print(f"  {o}")

    if all_unresolved_places:
        print(f"\nNicht aufgelöste Orte ({len(all_unresolved_places)}):")
        for p in sorted(all_unresolved_places):
            print(f"  {p}")


if __name__ == "__main__":
    main()

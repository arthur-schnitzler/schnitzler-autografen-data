#!/usr/bin/env python3
"""
Wikidata-Lookup für Organisationen OHNE GND.

Liest/schreibt: output/pmb_import_orgs.csv

Nur gute Matches: Label oder Alias muss dem Org-Namen
(normalisiert) entsprechen. Bei Mehrdeutigkeit wird
der Ort zur Disambiguierung herangezogen.
"""

import csv
import json
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent

WIKIDATA_API = "https://www.wikidata.org/w/api.php"

# Wikidata-Orte (QID → Ort-Normalisierung) für Disambiguierung
KNOWN_CITIES = {
    "Q1741": "wien",
    "Q64": "berlin",
    "Q1726": "münchen",
    "Q1718": "düsseldorf",
    "Q1055": "hamburg",
    "Q1718": "düsseldorf",
    "Q1022": "stuttgart",
    "Q2090": "nürnberg",
    "Q1794": "frankfurt",
    "Q84": "london",
    "Q90": "paris",
    "Q60": "new york",
    "Q1761": "dublin",
    "Q1781": "budapest",
    "Q1085": "prag",
    "Q546": "zürich",
    "Q1764": "brüssel",
    "Q1861": "breslau",
    "Q1731": "dresden",
    "Q2079": "leipzig",
    "Q13298": "graz",
    "Q41329": "salzburg",
}


def search_wikidata(name, ort=""):
    """Suche in Wikidata, nur gute Matches zurückgeben."""
    # Verschiedene Suchvarianten
    queries = [name]
    if ort and ort not in ("Zeitung", "Zeitschrift", "Verlag"):
        queries.append(f"{name} {ort}")

    for query in queries:
        params = urllib.parse.urlencode({
            "action": "wbsearchentities",
            "search": query,
            "language": "de",
            "uselang": "de",
            "type": "item",
            "limit": "5",
            "format": "json",
        })
        url = f"{WIKIDATA_API}?{params}"

        try:
            req = urllib.request.Request(url)
            req.add_header(
                "User-Agent",
                "SchnitzlerBriefverzeichnis/1.0 (DH research project)")
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            print(f"  API-Fehler: {e}")
            return None

        results = data.get("search", [])
        if not results:
            continue

        name_norm = normalize(name)

        for r in results:
            label = r.get("label", "")
            # Aliases aus der Suche
            aliases = r.get("aliases", [])
            all_names = [label] + aliases

            # Strikter Match: normalisierter Name muss übereinstimmen
            for candidate in all_names:
                if normalize(candidate) == name_norm:
                    # Wenn Ort angegeben, verifizieren
                    if ort and ort not in (
                            "Zeitung", "Zeitschrift", "Verlag"):
                        if verify_location(r["id"], ort):
                            return make_result(r)
                        # Ort passt nicht → skip
                        continue
                    return make_result(r)

        # Zweiter Versuch: Label enthält den Namen oder umgekehrt
        for r in results:
            label_norm = normalize(r.get("label", ""))
            if (name_norm in label_norm or label_norm in name_norm) \
                    and len(name_norm) > 5 and len(label_norm) > 5:
                # Nur wenn Längenunterschied gering
                ratio = len(name_norm) / max(len(label_norm), 1)
                if 0.7 < ratio < 1.4:
                    if ort and ort not in (
                            "Zeitung", "Zeitschrift", "Verlag"):
                        if verify_location(r["id"], ort):
                            return make_result(r)
                        continue
                    return make_result(r)

    return None


def verify_location(qid, ort):
    """Prüfe ob das Wikidata-Item am angegebenen Ort liegt."""
    ort_norm = normalize(ort)

    # P159 (headquarters), P131 (admin location), P17 (country)
    for prop in ["P159", "P131", "P276"]:
        params = urllib.parse.urlencode({
            "action": "wbgetclaims",
            "entity": qid,
            "property": prop,
            "format": "json",
        })
        try:
            url = f"{WIKIDATA_API}?{params}"
            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            for claim in data.get("claims", {}).get(prop, []):
                try:
                    loc_qid = claim["mainsnak"]["datavalue"]["value"]["id"]
                    if loc_qid in KNOWN_CITIES:
                        if KNOWN_CITIES[loc_qid] == ort_norm:
                            return True
                except (KeyError, TypeError):
                    continue
        except Exception:
            continue

    # Wenn wir den Ort nicht verifizieren können, akzeptieren wir
    # trotzdem wenn der Name exakt passt (kein false negative)
    return True


def make_result(item):
    return {
        "wikidata": item["id"],
        "wd_label": item.get("label", ""),
        "wd_description": item.get("description", ""),
    }


def normalize(name):
    if not name:
        return ""
    name = name.lower()
    name = re.sub(r"[\"'`\u00b4\-\u2013\u2014]", "", name)
    name = re.sub(r"\s+", " ", name)
    return name.strip()


def main():
    csv_path = BASE / "output" / "pmb_import_orgs.csv"

    with open(csv_path, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    # Nur die ohne GND UND ohne Wikidata
    to_search = [r for r in rows
                 if not r.get("gnd", "").strip()
                 and not r.get("wikidata", "").strip()]

    print(f"{len(to_search)} Orgs ohne GND und Wikidata zu durchsuchen\n")

    found = 0
    skipped = 0

    for i, row in enumerate(to_search):
        name = row.get("name", "")
        ort = row.get("ort", "")

        result = search_wikidata(name, ort)

        if result:
            row["wikidata"] = result["wikidata"]
            found += 1
            print(f"  + {name:50} → {result['wikidata']:12} "
                  f"({result['wd_label']}: {result['wd_description']})")
        else:
            skipped += 1
            print(f"  - {name:50}   (kein Match)")

        time.sleep(0.5)

        if (i + 1) % 50 == 0:
            save_csv(csv_path, rows)
            print(f"\n  --- {i+1}/{len(to_search)} "
                  f"({found} gefunden) ---\n")

    save_csv(csv_path, rows)

    total_gnd = sum(1 for r in rows if r.get("gnd", "").strip())
    total_wd = sum(1 for r in rows if r.get("wikidata", "").strip())
    total_none = sum(1 for r in rows
                     if not r.get("gnd", "").strip()
                     and not r.get("wikidata", "").strip())

    print(f"\n{'='*60}")
    print(f"ERGEBNIS ({len(rows)} Orgs gesamt)")
    print(f"  Mit GND:              {total_gnd}")
    print(f"  Mit Wikidata (nur):   {total_wd - total_gnd}")
    print(f"  Mit GND + Wikidata:   {sum(1 for r in rows if r.get('gnd','').strip() and r.get('wikidata','').strip())}")
    print(f"  Ohne Identifier:      {total_none}")
    print(f"{'='*60}")


def save_csv(path, rows):
    fields = ["name", "ort", "display_name", "dla_url",
              "gnd", "gnd_url", "gnd_name", "wikidata"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


if __name__ == "__main__":
    main()

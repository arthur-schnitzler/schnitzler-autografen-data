#!/usr/bin/env python3
"""
GND/Wikidata-Lookup für Organisationen via lobid.org API.

Liest:  output/pmb_import_orgs.csv
Schreibt: output/pmb_import_orgs.csv (erweitert um gnd, gnd_url, wikidata)

Verwendet die lobid.org GND-API (CorporateBody-Filter).
Rate-Limit: 1 Request/Sekunde.
"""

import csv
import json
import re
import time
import urllib.parse
import urllib.request
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
LOBID_URL = "https://lobid.org/gnd/search"


def search_gnd(name, ort=""):
    """Suche eine Organisation in der GND via lobid.org."""
    # Suchbegriff aufbauen
    query = name
    if ort and ort not in ("Zeitung", "Zeitschrift", "Verlag"):
        query = f"{name} {ort}"

    params = urllib.parse.urlencode({
        "q": query,
        "filter": "type:CorporateBody",
        "format": "json",
        "size": "5",
    })

    url = f"{LOBID_URL}?{params}"

    try:
        req = urllib.request.Request(url)
        req.add_header("Accept", "application/json")
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"  API-Fehler: {e}")
        return None

    members = data.get("member", [])
    if not members:
        return None

    # Best match finden
    name_norm = normalize(name)

    for m in members:
        pref = m.get("preferredName", "")
        # Auch Varianten prüfen
        variants = [pref] + [
            v.get("label", "") if isinstance(v, dict) else str(v)
            for v in m.get("variantName", [])
        ]

        for v in variants:
            if normalize(v) == name_norm:
                return extract_ids(m)

    # Fallback: erster Treffer wenn Name sehr ähnlich
    pref = members[0].get("preferredName", "")
    if name_norm in normalize(pref) or normalize(pref) in name_norm:
        return extract_ids(members[0])

    return None


def extract_ids(member):
    """GND-ID und Wikidata aus lobid-Ergebnis extrahieren."""
    gnd = member.get("gndIdentifier", "")
    wikidata = ""
    for sa in member.get("sameAs", []):
        sa_id = sa.get("id", "") if isinstance(sa, dict) else str(sa)
        if "wikidata.org" in sa_id:
            wikidata = sa_id.split("/")[-1]
            break

    return {
        "gnd": gnd,
        "gnd_url": f"https://d-nb.info/gnd/{gnd}" if gnd else "",
        "gnd_name": member.get("preferredName", ""),
        "wikidata": wikidata,
    }


def normalize(name):
    """Name für Vergleich normalisieren."""
    name = name.lower()
    name = re.sub(r"[\"''`´\-–]", "", name)
    name = re.sub(r"\s+", " ", name)
    return name.strip()


def main():
    csv_path = BASE / "output" / "pmb_import_orgs.csv"

    with open(csv_path, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    print(f"Suche GND/Wikidata für {len(rows)} Organisationen...\n")

    found = 0
    not_found = 0

    for i, row in enumerate(rows):
        name = row.get("name", "")
        ort = row.get("ort", "")

        # Bereits gefunden? (bei erneutem Lauf)
        if row.get("gnd", "").strip():
            found += 1
            continue

        result = search_gnd(name, ort)

        if result:
            row["gnd"] = result["gnd"]
            row["gnd_url"] = result["gnd_url"]
            row["gnd_name"] = result["gnd_name"]
            row["wikidata"] = result["wikidata"]
            found += 1
            print(f"  ✓ {name:50} → GND {result['gnd']}  "
                  f"({result['gnd_name']})"
                  f"{'  WD:' + result['wikidata'] if result['wikidata'] else ''}")
        else:
            row.setdefault("gnd", "")
            row.setdefault("gnd_url", "")
            row.setdefault("gnd_name", "")
            row.setdefault("wikidata", "")
            not_found += 1
            print(f"  ✗ {name:50}   (kein Treffer)")

        # Rate limit
        time.sleep(0.5)

        # Zwischenspeichern alle 50
        if (i + 1) % 50 == 0:
            save_csv(csv_path, rows)
            print(f"\n  --- Zwischenstand: {i+1}/{len(rows)} "
                  f"({found} gefunden, {not_found} nicht) ---\n")

    save_csv(csv_path, rows)

    print(f"\n{'='*60}")
    print(f"ERGEBNIS: {found} mit GND, {not_found} ohne GND")
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

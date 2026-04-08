#!/usr/bin/env python3
"""
Download aller Datensätze aus dem Schnitzler-Bestand (BF00000730)
des DLA Marbach via Data+ API.

Erzeugt zwei Dateien:
  schnitzler_als_autor.json   – alles, wo Schnitzler Verfasser ist
                                (Briefe von, Manuskripte, Dokumente)
  briefe_an_schnitzler.json   – alle Briefe an Schnitzler
                                (Briefe an, Briefe anderer)

Zusätzlich wird der vollständige Rohdaten-Dump gespeichert:
  schnitzler_bestand_komplett.json
"""

import json
import sys
import time
import urllib.request
from pathlib import Path

BASE_URL = "https://dataservice.dla-marbach.de/v1/records"
COLLECTION = "BF00000730"  # A:Schnitzler, Arthur
PAGE_SIZE = 200  # kleiner als API-Max (500), um Abschneidung zu vermeiden

# Kategorien, in denen Schnitzler Autor/Verfasser ist
VON_SCHNITZLER = {"Briefe von", "Manuskripte", "Dokumente"}
# Kategorien für Korrespondenz an Schnitzler
AN_SCHNITZLER = {"Briefe an", "Briefe anderer"}
# Rest (Manuskripte anderer) kommt in beide NICHT, aber in den Komplett-Dump


def fetch_all():
    """Alle Datensätze der Sammlung paginiert laden."""
    records = []
    offset = 1

    # Erst Gesamtzahl ermitteln
    count_url = (
        f"{BASE_URL}/count"
        f"?q=collection_id_mv:{COLLECTION}"
    )
    with urllib.request.urlopen(count_url, timeout=30) as resp:
        body = json.loads(resp.read().decode("utf-8"))
        total = body.get("documentCount", body) if isinstance(body, dict) else int(body)
    print(f"Gesamtanzahl Datensätze: {total}")

    while offset <= total:
        url = (
            f"{BASE_URL}"
            f"?q=collection_id_mv:{COLLECTION}"
            f"&format=jsonl&from={offset}&size={PAGE_SIZE}"
        )
        print(f"  Lade {offset}–{min(offset + PAGE_SIZE - 1, total)} "
              f"von {total} ...", end=" ", flush=True)

        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=120) as resp:
            raw = resp.read().decode("utf-8")
        batch = []
        skipped_positions = []
        lines = raw.strip().split("\n")
        for idx, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
            try:
                batch.append(json.loads(line))
            except json.JSONDecodeError:
                # Abgeschnittene letzte Zeile
                skipped_positions.append(offset + idx)

        # Fehlende Einträge einzeln nachladen (json-Format für Einzelabruf)
        for pos in skipped_positions:
            retry_url = (
                f"{BASE_URL}"
                f"?q=collection_id_mv:{COLLECTION}"
                f"&format=json&from={pos}&size=1"
            )
            try:
                with urllib.request.urlopen(retry_url, timeout=30) as rr:
                    single = json.loads(rr.read().decode("utf-8"))
                if isinstance(single, list) and single:
                    batch.append(single[0])
                elif isinstance(single, dict):
                    batch.append(single)
            except Exception:
                print(f"\n  WARNUNG: Eintrag {pos} nicht ladbar", end="")

        if not batch:
            break

        records.extend(batch)
        print(f"({len(batch)} Datensätze)")

        offset += PAGE_SIZE
        time.sleep(0.5)  # Rate-Limit respektieren

    print(f"\nInsgesamt geladen: {len(records)}")
    return records


def split_and_save(records, out_dir):
    """Datensätze aufteilen und als JSON speichern."""
    von = []
    an = []
    andere = []

    for rec in records:
        cat = rec.get("category", "")
        if cat in VON_SCHNITZLER:
            von.append(rec)
        elif cat in AN_SCHNITZLER:
            an.append(rec)
        else:
            andere.append(rec)

    # Komplett-Dump
    komplett_path = out_dir / "schnitzler_bestand_komplett.json"
    komplett_path.write_text(
        json.dumps(records, indent=2, ensure_ascii=False),
        encoding="utf-8")
    print(f"\n{komplett_path.name}: {len(records)} Datensätze (komplett)")

    # Schnitzler als Autor
    von_path = out_dir / "schnitzler_als_autor.json"
    von_path.write_text(
        json.dumps(von, indent=2, ensure_ascii=False),
        encoding="utf-8")
    print(f"{von_path.name}: {len(von)} Datensätze")
    for cat in sorted(VON_SCHNITZLER):
        n = sum(1 for r in von if r.get("category") == cat)
        if n:
            print(f"  {cat}: {n}")

    # Briefe an Schnitzler
    an_path = out_dir / "briefe_an_schnitzler.json"
    an_path.write_text(
        json.dumps(an, indent=2, ensure_ascii=False),
        encoding="utf-8")
    print(f"{an_path.name}: {len(an)} Datensätze")
    for cat in sorted(AN_SCHNITZLER):
        n = sum(1 for r in an if r.get("category") == cat)
        if n:
            print(f"  {cat}: {n}")

    if andere:
        print(f"\nNicht zugeordnet ({len(andere)} Datensätze):")
        from collections import Counter
        for cat, n in Counter(
                r.get("category", "(leer)") for r in andere).most_common():
            print(f"  {cat}: {n}")


def main():
    out_dir = Path(__file__).resolve().parent.parent
    print("DLA Marbach Data+ Download")
    print(f"Sammlung: {COLLECTION} (A:Schnitzler, Arthur)\n")

    try:
        records = fetch_all()
    except Exception as e:
        print(f"\nFehler beim Download: {e}", file=sys.stderr)
        sys.exit(1)

    split_and_save(records, out_dir)
    print("\nFertig.")


if __name__ == "__main__":
    main()

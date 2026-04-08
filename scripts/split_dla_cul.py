#!/usr/bin/env python3
"""
Splittet die DLA-Downloads in DLA- und CUL-Dateien auf.

CUL-Einträge werden erkannt am Prefix "Cambridge." in accessionNumber.
Die DLA-URL bleibt erhalten, dazu kommt ein CUL-URI-Feld (vorerst leer,
da die CUL-ArchivesSpace-API nicht öffentlich zugänglich ist).

Erzeugt:
  dla_schnitzler_als_autor.json
  dla_briefe_an_schnitzler.json
  cul_schnitzler_als_autor.json
  cul_briefe_an_schnitzler.json

Die alten Dateien werden entfernt.
"""

import json
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent

# Kategorien
VON_SCHNITZLER = {"Briefe von", "Manuskripte", "Dokumente"}
AN_SCHNITZLER = {"Briefe an", "Briefe anderer"}


def is_cul(record):
    return record.get("accessionNumber", "").startswith("Cambridge.")


def add_cul_fields(record):
    """Fügt CUL-spezifische Felder hinzu."""
    acc = record.get("accessionNumber", "")
    record["standort"] = "CUL"
    record["cul_signatur"] = acc
    # CUL-URI kann derzeit nicht automatisch ermittelt werden
    # (ArchivesSpace hinter AWS WAF).
    # Basis-URL für manuelle Ergänzung:
    # https://archivesearch.lib.cam.ac.uk/repositories/2/archival_objects/{id}
    record["cul_uri"] = ""
    # DLA-URL bleibt im "url"-Feld erhalten
    record["dla_url"] = record.get("url", "")
    return record


def add_dla_fields(record):
    """Markiert DLA-Herkunft explizit."""
    record["standort"] = "DLA"
    record["dla_url"] = record.get("url", "")
    return record


def save(path, records, label):
    path.write_text(
        json.dumps(records, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"  {path.name}: {len(records)} ({label})")


def main():
    # Lade Komplett-Dump
    komplett = json.loads(
        (BASE / "schnitzler_bestand_komplett.json").read_text(encoding="utf-8"))
    print(f"Komplett-Dump: {len(komplett)} Datensätze\n")

    # Aufteilen nach Standort und Kategorie
    dla_von, dla_an = [], []
    cul_von, cul_an = [], []
    rest_dla, rest_cul = [], []

    for rec in komplett:
        cat = rec.get("category", "")
        cul = is_cul(rec)

        if cat in VON_SCHNITZLER:
            if cul:
                cul_von.append(add_cul_fields(rec))
            else:
                dla_von.append(add_dla_fields(rec))
        elif cat in AN_SCHNITZLER:
            if cul:
                cul_an.append(add_cul_fields(rec))
            else:
                dla_an.append(add_dla_fields(rec))
        else:
            # Manuskripte anderer, Dokumente anderer, Briefwechsel
            if cul:
                rest_cul.append(add_cul_fields(rec))
            else:
                rest_dla.append(add_dla_fields(rec))

    print("DLA Marbach:")
    save(BASE / "dla_schnitzler_als_autor.json", dla_von,
         f"Briefe von: {sum(1 for r in dla_von if r['category']=='Briefe von')}, "
         f"Manuskripte: {sum(1 for r in dla_von if r['category']=='Manuskripte')}, "
         f"Dokumente: {sum(1 for r in dla_von if r['category']=='Dokumente')}")
    save(BASE / "dla_briefe_an_schnitzler.json", dla_an,
         f"Briefe an: {sum(1 for r in dla_an if r['category']=='Briefe an')}, "
         f"Briefe anderer: {sum(1 for r in dla_an if r['category']=='Briefe anderer')}")

    print("\nCambridge University Library:")
    save(BASE / "cul_schnitzler_als_autor.json", cul_von,
         f"Briefe von: {sum(1 for r in cul_von if r['category']=='Briefe von')}, "
         f"Manuskripte: {sum(1 for r in cul_von if r['category']=='Manuskripte')}, "
         f"Dokumente: {sum(1 for r in cul_von if r['category']=='Dokumente')}")
    save(BASE / "cul_briefe_an_schnitzler.json", cul_an,
         f"Briefe an: {sum(1 for r in cul_an if r['category']=='Briefe an')}, "
         f"Briefe anderer: {sum(1 for r in cul_an if r['category']=='Briefe anderer')}")

    if rest_dla or rest_cul:
        print(f"\nNicht zugeordnet: {len(rest_dla)} DLA, {len(rest_cul)} CUL")

    # Alte Dateien entfernen
    for old in ["schnitzler_als_autor.json", "briefe_an_schnitzler.json"]:
        p = BASE / old
        if p.exists():
            p.unlink()
            print(f"\nEntfernt: {old}")

    print("\nFertig.")


if __name__ == "__main__":
    main()

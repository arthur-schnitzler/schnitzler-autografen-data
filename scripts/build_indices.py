#!/usr/bin/env python3
"""
Erzeugt gekürzte Versionen von pmb-export/listperson.xml und
pmb-export/listplace.xml in data/indices/, die nur Einträge enthalten,
die tatsächlich in den TEI-Editionen referenziert werden.
"""

import glob
import re
import xml.etree.ElementTree as ET
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
NS = {"tei": "http://www.tei-c.org/ns/1.0"}
ET.register_namespace("", "http://www.tei-c.org/ns/1.0")


def collect_referenced_ids(editions_dir):
    """Sammelt alle PMB-IDs, die in den TEI-Dateien vorkommen."""
    person_ids = set()
    place_ids = set()

    for f in glob.glob(str(editions_dir / "*.xml")):
        with open(f) as fh:
            content = fh.read()
        person_ids.update(re.findall(r'<persName ref="#pmb(\d+)"', content))
        person_ids.update(re.findall(r'<author ref="#pmb(\d+)"', content))
        person_ids.update(re.findall(r'target="correspondence_(\d+)"', content))
        place_ids.update(re.findall(r'<placeName ref="#pmb(\d+)"', content))

    return person_ids, place_ids


def filter_list(source_path, out_path, tag, id_prefix, keep_ids):
    """Filtert eine PMB-list*.xml auf die angegebenen IDs."""
    tree = ET.parse(source_path)
    root = tree.getroot()

    # Finde das listPerson/listPlace-Element
    list_el = root.find(f".//{{{NS['tei']}}}{tag}")
    if list_el is None:
        print(f"  ⚠ <{tag}> nicht gefunden in {source_path}")
        return 0

    # Einträge filtern
    child_tag = tag.replace("list", "").lower()  # listPerson → person
    removed = 0
    kept = 0
    for el in list(list_el):
        xml_id = el.get("{http://www.w3.org/XML/1998/namespace}id", "")
        pmb_id = xml_id.replace(f"{id_prefix}__", "")
        if pmb_id in keep_ids:
            kept += 1
        else:
            list_el.remove(el)
            removed += 1

    # Schreiben
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tree.write(out_path, encoding="unicode", xml_declaration=True)

    # Nachformatierung: Zeilenumbruch nach XML-Deklaration
    text = out_path.read_text("utf-8")
    text = text.replace("?><", "?>\n<", 1)
    out_path.write_text(text, "utf-8")

    return kept


def main():
    editions_dir = BASE / "data" / "editions"
    indices_dir = BASE / "data" / "indices"

    print("Sammle referenzierte PMB-IDs aus TEI-Dateien …")
    person_ids, place_ids = collect_referenced_ids(editions_dir)
    print(f"  {len(person_ids)} Personen, {len(place_ids)} Orte")

    print("Filtere listperson.xml …")
    kept = filter_list(
        BASE / "pmb-export" / "listperson.xml",
        indices_dir / "listperson.xml",
        "listPerson", "person", person_ids)
    print(f"  {kept} Personen übernommen")

    print("Filtere listplace.xml …")
    kept = filter_list(
        BASE / "pmb-export" / "listplace.xml",
        indices_dir / "listplace.xml",
        "listPlace", "place", place_ids)
    print(f"  {kept} Orte übernommen")

    print(f"\nDateien geschrieben nach {indices_dir}")


if __name__ == "__main__":
    main()

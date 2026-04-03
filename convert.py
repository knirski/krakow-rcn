#!/usr/bin/env python3
"""
Convert RCN GML to GeoPackage with resolved cross-references.

Creates a single .gpkg file with all original layers plus join tables
that resolve the xlink:href cross-references between features.
"""

import sqlite3
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

NS = {
    "rcn": "urn:gugik:specyfikacje:gmlas:rejestrcennieruchomosci:1.0",
    "gml": "http://www.opengis.net/gml/3.2",
    "xlink": "http://www.w3.org/1999/xlink",
}


def find_gml(directory):
    gmls = list(Path(directory).glob("*.gml"))
    if not gmls:
        print("ERROR: No .gml file found", file=sys.stderr)
        sys.exit(1)
    return gmls[0]


def export_layers(gml_path, gpkg_path):
    """Export all layers from GML to GeoPackage using ogr2ogr."""
    if gpkg_path.exists():
        gpkg_path.unlink()

    layers = [
        "RCN_Transakcja", "RCN_Dokument", "RCN_Nieruchomosc",
        "RCN_Dzialka", "RCN_Budynek", "RCN_Lokal", "RCN_Adres",
    ]

    for i, layer in enumerate(layers):
        print(f"  Exporting {layer}...")
        cmd = [
            "ogr2ogr", "-f", "GPKG",
            str(gpkg_path), str(gml_path), layer,
            "-t_srs", "EPSG:4326", "-nln", layer,
        ]
        if i > 0:
            cmd.insert(3, "-update")
        subprocess.run(cmd, check=True)


def extract_xlinks(gml_path):
    """Stream-parse GML to extract xlink cross-references."""
    print("  Streaming GML to extract xlink references...")

    links = {
        "trans_nieruchomosc": [], "trans_dokument": [],
        "nier_dzialka": [], "nier_budynek": [], "nier_lokal": [],
        "dzialka_adres": [], "budynek_adres": [], "lokal_adres": [],
    }

    current_parent_tag = None
    current_parent_id = None
    count = 0

    HREF = f"{{{NS['xlink']}}}href"
    GML_ID = f"{{{NS['gml']}}}id"

    parent_tags = {
        f"{{{NS['rcn']}}}RCN_Transakcja",
        f"{{{NS['rcn']}}}RCN_Nieruchomosc",
        f"{{{NS['rcn']}}}RCN_Dzialka",
        f"{{{NS['rcn']}}}RCN_Budynek",
        f"{{{NS['rcn']}}}RCN_Lokal",
    }

    child_map = {
        ("RCN_Transakcja", "nieruchomosc"): "trans_nieruchomosc",
        ("RCN_Transakcja", "podstawaPrawna"): "trans_dokument",
        ("RCN_Nieruchomosc", "dzialka"): "nier_dzialka",
        ("RCN_Nieruchomosc", "budynek"): "nier_budynek",
        ("RCN_Nieruchomosc", "lokal"): "nier_lokal",
        ("RCN_Dzialka", "adresDzialki"): "dzialka_adres",
        ("RCN_Budynek", "adresBudynku"): "budynek_adres",
        ("RCN_Lokal", "adresBudynkuZLokalem"): "lokal_adres",
    }

    child_tags = {f"{{{NS['rcn']}}}{name.split(',')[-1]}" for _, name in
                  [(k[1], k[1]) for k in child_map.keys()]}

    for event, elem in ET.iterparse(str(gml_path), events=("start", "end")):
        if event == "start":
            if elem.tag in parent_tags:
                current_parent_tag = elem.tag.split("}")[-1]
                current_parent_id = elem.get(GML_ID, "")
            else:
                href = elem.get(HREF)
                if href and current_parent_id:
                    tag_local = elem.tag.split("}")[-1]
                    key = child_map.get((current_parent_tag, tag_local))
                    if key:
                        links[key].append((current_parent_id, href))

        elif event == "end":
            if elem.tag in parent_tags:
                current_parent_tag = None
                current_parent_id = None
                count += 1
                if count % 500000 == 0:
                    print(f"    {count:,} features...")
                elem.clear()
            elif elem.tag == f"{{{NS['gml']}}}featureMember":
                elem.clear()

    print(f"    Done: {count:,} features")
    for k, v in links.items():
        if v:
            print(f"    {k}: {len(v):,}")
    return links


def create_joins(gpkg_path, links):
    """Create join tables and indexes in the GeoPackage."""
    conn = sqlite3.connect(str(gpkg_path))
    c = conn.cursor()

    tables = {
        "xlink_trans_nieruchomosc": ("transakcja_id", "nieruchomosc_id"),
        "xlink_trans_dokument": ("transakcja_id", "dokument_id"),
        "xlink_nier_dzialka": ("nieruchomosc_id", "dzialka_id"),
        "xlink_nier_budynek": ("nieruchomosc_id", "budynek_id"),
        "xlink_nier_lokal": ("nieruchomosc_id", "lokal_id"),
        "xlink_dzialka_adres": ("dzialka_id", "adres_id"),
        "xlink_budynek_adres": ("budynek_id", "adres_id"),
        "xlink_lokal_adres": ("lokal_id", "adres_id"),
    }

    link_keys = [
        "trans_nieruchomosc", "trans_dokument",
        "nier_dzialka", "nier_budynek", "nier_lokal",
        "dzialka_adres", "budynek_adres", "lokal_adres",
    ]

    for (table, (col1, col2)), key in zip(tables.items(), link_keys):
        c.execute(f"DROP TABLE IF EXISTS {table}")
        c.execute(f"CREATE TABLE {table} ({col1} TEXT NOT NULL, {col2} TEXT NOT NULL)")
        data = links[key]
        if data:
            c.executemany(f"INSERT INTO {table} ({col1}, {col2}) VALUES (?, ?)", data)
            c.execute(f"CREATE INDEX idx_{table}_{col1} ON {table}({col1})")
            c.execute(f"CREATE INDEX idx_{table}_{col2} ON {table}({col2})")
        print(f"    {table}: {len(data):,} rows")

    for layer in ["RCN_Transakcja", "RCN_Nieruchomosc", "RCN_Dzialka",
                   "RCN_Budynek", "RCN_Lokal", "RCN_Dokument", "RCN_Adres"]:
        try:
            c.execute(f'CREATE INDEX IF NOT EXISTS idx_{layer}_gml_id ON "{layer}"(gml_id)')
        except Exception as e:
            print(f"    Warning indexing {layer}: {e}")

    conn.commit()
    conn.close()


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <input.gml or directory> <output.gpkg>")
        sys.exit(1)

    gml_input = Path(sys.argv[1])
    gpkg_path = Path(sys.argv[2])

    if gml_input.is_dir():
        gml_path = find_gml(gml_input)
    else:
        gml_path = gml_input

    print(f"Input:  {gml_path} ({gml_path.stat().st_size / 1e9:.1f} GB)")
    print(f"Output: {gpkg_path}")

    print("\nStep 1/3: Exporting layers via ogr2ogr...")
    export_layers(gml_path, gpkg_path)

    print("\nStep 2/3: Extracting xlink cross-references...")
    xlinks = extract_xlinks(gml_path)

    print("\nStep 3/3: Creating join tables...")
    create_joins(gpkg_path, xlinks)

    print(f"\nDone! {gpkg_path} ({gpkg_path.stat().st_size / 1e6:.0f} MB)")


if __name__ == "__main__":
    main()

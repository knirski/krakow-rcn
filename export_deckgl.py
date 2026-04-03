#!/usr/bin/env python3
"""
Export transaction data from GPKG as deck.gl HTML + gzipped JSON data files.
Includes all properties from every joined table.
"""

import gzip
import json
import sqlite3
import struct
import sys
from pathlib import Path

RODZAJ_TRANSAKCJI = {1: "sprzedaż", 2: "zamiana", 3: "oddanie w użytkowanie wieczyste",
                     4: "wniesienie aportem", 5: "zniesienie współwłasności", 6: "inna"}
RODZAJ_RYNKU = {1: "pierwotny", 2: "wtórny"}
STRONA = {1: "Skarb Państwa", 2: "jednostka samorządu", 3: "osoba fizyczna",
           4: "osoba prawna", 5: "inne"}
RODZAJ_NIERUCHOMOSCI = {1: "gruntowa", 2: "budynkowa", 3: "gruntowa zabudowana", 4: "lokalowa"}
FUNKCJA_LOKALU = {1: "mieszkalny", 2: "użytkowy"}
RODZAJ_BUDYNKU = {
    110: "mieszkalny", 111: "dom jednorodzinny wolnostojący",
    112: "dom jednorodzinny w zabudowie bliźniaczej", 113: "dom jednorodzinny w zabudowie szeregowej",
    114: "wielomieszkaniowy", 115: "dom wielomieszkaniowy (blok)", 116: "wieżowiec",
    120: "przemysłowy", 121: "fabryczny", 130: "handlowo-usługowy",
    140: "biurowy", 150: "szpitalny", 160: "oświatowy", 170: "sakralny",
    180: "gospodarczy", 181: "garaż", 182: "wiata", 190: "inny",
}
SPOSOB_UZYTKOWANIA = {
    1: "R - grunty orne", 2: "S - sady", 3: "B - tereny mieszkaniowe",
    4: "Ba - tereny przemysłowe", 5: "Bi - inne tereny zabudowane",
    6: "Bp - zurbanizowane niezabudowane", 7: "Bz - tereny rekreacyjne",
    8: "K - użytki kopalne", 9: "dr - drogi",
    10: "Tk - tereny kolejowe", 11: "Ti - inne tereny komunikacyjne",
    12: "Tp - grunty przeznaczone pod budowę dróg",
    13: "Ł - łąki trwałe", 14: "Ps - pastwiska",
    15: "Ls - lasy", 16: "Lz - grunty zadrzewione",
    17: "W - rowy", 18: "N - nieużytki",
    19: "Wp - wody płynące", 20: "Ws - wody stojące",
    21: "Tr - tereny różne",
}
RODZAJ_PRAWA = {1: "własność", 2: "użytkowanie wieczyste", 3: "spółdzielcze własnościowe"}


def gpkg_geom_to_lonlat(blob):
    if not blob:
        return None, None
    flags = blob[3]
    envelope_type = (flags >> 1) & 0x07
    envelope_sizes = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}
    envelope_size = envelope_sizes.get(envelope_type, 0)
    wkb_offset = 8 + envelope_size
    if envelope_type >= 1:
        min_x, max_x, min_y, max_y = struct.unpack_from('<dddd', blob, 8)
        return (min_x + max_x) / 2, (min_y + max_y) / 2
    wkb = blob[wkb_offset:]
    if len(wkb) < 21:
        return None, None
    byte_order = '<' if wkb[0] == 1 else '>'
    geom_type = struct.unpack_from(f'{byte_order}I', wkb, 1)[0]
    if geom_type == 1:
        x, y = struct.unpack_from(f'{byte_order}dd', wkb, 5)
        return x, y
    return None, None


def compact(d):
    return {k: v for k, v in d.items() if v is not None}


def lookup(mapping, val):
    if val is None:
        return None
    return mapping.get(val, str(val))


def export_lokale(conn):
    print("  Exporting lokale (apartments)...")
    rows = conn.execute("""
        SELECT
            t.oznaczenieTransakcji,
            t.rodzajTransakcji, t.rodzajRynku,
            t.stronaSprzedajaca, t.stronaKupujaca,
            t.cenaTransakcjiBrutto, t.kwotaPodatkuVAT,
            n.rodzajNieruchomosci, n.cenaNieruchomosciBrutto,
            n.rodzajPrawaDoNieruchomosci, n.udzialWPrawieDoNieruchomosci,
            n.polePowierzchniNieruchomosciGruntowej, n.opis,
            l.idLokalu, l.funkcjaLokalu, l.liczbaIzb, l.nrKondygnacji,
            l.powUzytkowaLokalu, l.cenaLokaluBrutto,
            l.powUzytkowaPomieszczenPrzynal, l.dodatkoweInformacje, l.kwotaPodatkuVAT,
            dok.oznaczenieDokumentu, dok.dataSporzadzeniaDokumentu, dok.tworcaDokumentu,
            a.miejscowosc, a.ulica, a.numerPorzadkowy,
            l.geom
        FROM RCN_Transakcja t
        JOIN xlink_trans_nieruchomosc tn ON t.gml_id = tn.transakcja_id
        JOIN RCN_Nieruchomosc n ON n.gml_id = tn.nieruchomosc_id
        JOIN xlink_nier_lokal nl ON n.gml_id = nl.nieruchomosc_id
        JOIN RCN_Lokal l ON l.gml_id = nl.lokal_id
        LEFT JOIN xlink_trans_dokument td ON t.gml_id = td.transakcja_id
        LEFT JOIN RCN_Dokument dok ON dok.gml_id = td.dokument_id
        LEFT JOIN (
            SELECT la.lokal_id, a.miejscowosc, a.ulica, a.numerPorzadkowy
            FROM xlink_lokal_adres la
            JOIN RCN_Adres a ON a.gml_id = la.adres_id
            GROUP BY la.lokal_id
        ) a ON l.gml_id = a.lokal_id
        WHERE l.geom IS NOT NULL
    """).fetchall()

    features = []
    for r in rows:
        lon, lat = gpkg_geom_to_lonlat(r[28])
        if lon is None:
            continue
        pow_m2 = r[17]
        cena = r[5] or r[18] or r[8]
        cena_m2 = round(cena / pow_m2, 2) if (cena and pow_m2 and pow_m2 > 0) else None
        features.append(compact({
            "lon": round(lon, 5), "lat": round(lat, 5),
            "id_transakcji": r[0],
            "rodzaj_transakcji": lookup(RODZAJ_TRANSAKCJI, r[1]),
            "rynek": lookup(RODZAJ_RYNKU, r[2]),
            "sprzedajacy": lookup(STRONA, r[3]),
            "kupujacy": lookup(STRONA, r[4]),
            "cena_transakcji": r[5], "vat_transakcji": r[6],
            "rodzaj_nieruchomosci": lookup(RODZAJ_NIERUCHOMOSCI, r[7]),
            "cena_nieruchomosci": r[8],
            "prawo_do_nieruchomosci": lookup(RODZAJ_PRAWA, r[9]),
            "udzial_w_prawie": r[10],
            "pow_nieruchomosci_gruntowej_ha": r[11],
            "opis_nieruchomosci": r[12],
            "id_lokalu": r[13],
            "funkcja_lokalu": lookup(FUNKCJA_LOKALU, r[14]),
            "liczba_izb": r[15], "kondygnacja": r[16],
            "pow_uzytkowa_m2": r[17], "cena_lokalu": r[18],
            "pow_pomieszczen_przynaleznych_m2": r[19],
            "dodatkowe_info_lokal": r[20], "vat_lokal": r[21],
            "cena_za_m2": cena_m2,
            "oznaczenie_dokumentu": r[22],
            "data_dokumentu": r[23], "notariusz": r[24],
            "miejscowosc": r[25], "ulica": r[26], "numer": r[27],
        }))
    print(f"    {len(features):,} apartment transactions")
    return features


def export_dzialki(conn):
    print("  Exporting dzialki (land plots)...")
    rows = conn.execute("""
        SELECT
            t.oznaczenieTransakcji,
            t.rodzajTransakcji, t.rodzajRynku,
            t.stronaSprzedajaca, t.stronaKupujaca,
            t.cenaTransakcjiBrutto, t.kwotaPodatkuVAT,
            n.rodzajNieruchomosci, n.cenaNieruchomosciBrutto,
            n.rodzajPrawaDoNieruchomosci, n.udzialWPrawieDoNieruchomosci,
            n.polePowierzchniNieruchomosciGruntowej, n.opis,
            d.idDzialki, d.przeznaczenieWMPZP, d.polePowierzchniEwidencyjnej,
            d.cenaDzialkiEwidencyjnejBrutto, d.sposobUzytkowania,
            d.dodatkoweInformacje, d.kwotaPodatkuVAT,
            dok.oznaczenieDokumentu, dok.dataSporzadzeniaDokumentu, dok.tworcaDokumentu,
            a.miejscowosc, a.ulica, a.numerPorzadkowy,
            d.geom
        FROM RCN_Transakcja t
        JOIN xlink_trans_nieruchomosc tn ON t.gml_id = tn.transakcja_id
        JOIN RCN_Nieruchomosc n ON n.gml_id = tn.nieruchomosc_id
        JOIN xlink_nier_dzialka nd ON n.gml_id = nd.nieruchomosc_id
        JOIN RCN_Dzialka d ON d.gml_id = nd.dzialka_id
        LEFT JOIN xlink_trans_dokument td ON t.gml_id = td.transakcja_id
        LEFT JOIN RCN_Dokument dok ON dok.gml_id = td.dokument_id
        LEFT JOIN (
            SELECT da.dzialka_id, a.miejscowosc, a.ulica, a.numerPorzadkowy
            FROM xlink_dzialka_adres da
            JOIN RCN_Adres a ON a.gml_id = da.adres_id
            GROUP BY da.dzialka_id
        ) a ON d.gml_id = a.dzialka_id
        WHERE d.geom IS NOT NULL
    """).fetchall()

    features = []
    for r in rows:
        lon, lat = gpkg_geom_to_lonlat(r[26])
        if lon is None:
            continue
        pow_ha = r[15]
        cena = r[5] or r[16] or r[8]
        cena_ha = round(cena / pow_ha, 2) if (cena and pow_ha and pow_ha > 0) else None
        cena_m2 = round(cena / (pow_ha * 10000), 2) if (cena and pow_ha and pow_ha > 0) else None
        features.append(compact({
            "lon": round(lon, 5), "lat": round(lat, 5),
            "id_transakcji": r[0],
            "rodzaj_transakcji": lookup(RODZAJ_TRANSAKCJI, r[1]),
            "rynek": lookup(RODZAJ_RYNKU, r[2]),
            "sprzedajacy": lookup(STRONA, r[3]),
            "kupujacy": lookup(STRONA, r[4]),
            "cena_transakcji": r[5], "vat_transakcji": r[6],
            "rodzaj_nieruchomosci": lookup(RODZAJ_NIERUCHOMOSCI, r[7]),
            "cena_nieruchomosci": r[8],
            "prawo_do_nieruchomosci": lookup(RODZAJ_PRAWA, r[9]),
            "udzial_w_prawie": r[10],
            "pow_nieruchomosci_gruntowej_ha": r[11],
            "opis_nieruchomosci": r[12],
            "id_dzialki": r[13], "przeznaczenie_mpzp": r[14],
            "pow_ewidencyjna_ha": r[15], "cena_dzialki": r[16],
            "sposob_uzytkowania": lookup(SPOSOB_UZYTKOWANIA, r[17]),
            "dodatkowe_info_dzialka": r[18], "vat_dzialka": r[19],
            "cena_za_ha": cena_ha, "cena_za_m2": cena_m2,
            "oznaczenie_dokumentu": r[20],
            "data_dokumentu": r[21], "notariusz": r[22],
            "miejscowosc": r[23], "ulica": r[24], "numer": r[25],
        }))
    print(f"    {len(features):,} land plot transactions")
    return features


def export_budynki(conn):
    print("  Exporting budynki (buildings)...")
    rows = conn.execute("""
        SELECT
            t.oznaczenieTransakcji,
            t.rodzajTransakcji, t.rodzajRynku,
            t.stronaSprzedajaca, t.stronaKupujaca,
            t.cenaTransakcjiBrutto, t.kwotaPodatkuVAT,
            n.rodzajNieruchomosci, n.cenaNieruchomosciBrutto,
            n.rodzajPrawaDoNieruchomosci, n.udzialWPrawieDoNieruchomosci,
            n.polePowierzchniNieruchomosciGruntowej, n.opis,
            b.idBudynku, b.powierzchniaUzytkowaBudynku,
            b.cenaBudynkuBrutto, b.rodzajBudynku,
            b.dodatkoweInformacje, b.kwotaPodatkuVAT,
            dok.oznaczenieDokumentu, dok.dataSporzadzeniaDokumentu, dok.tworcaDokumentu,
            a.miejscowosc, a.ulica, a.numerPorzadkowy,
            b.geom
        FROM RCN_Transakcja t
        JOIN xlink_trans_nieruchomosc tn ON t.gml_id = tn.transakcja_id
        JOIN RCN_Nieruchomosc n ON n.gml_id = tn.nieruchomosc_id
        JOIN xlink_nier_budynek nb ON n.gml_id = nb.nieruchomosc_id
        JOIN RCN_Budynek b ON b.gml_id = nb.budynek_id
        LEFT JOIN xlink_trans_dokument td ON t.gml_id = td.transakcja_id
        LEFT JOIN RCN_Dokument dok ON dok.gml_id = td.dokument_id
        LEFT JOIN (
            SELECT ba.budynek_id, a.miejscowosc, a.ulica, a.numerPorzadkowy
            FROM xlink_budynek_adres ba
            JOIN RCN_Adres a ON a.gml_id = ba.adres_id
            GROUP BY ba.budynek_id
        ) a ON b.gml_id = a.budynek_id
        WHERE b.geom IS NOT NULL
    """).fetchall()

    features = []
    for r in rows:
        lon, lat = gpkg_geom_to_lonlat(r[25])
        if lon is None:
            continue
        pow_m2 = r[14]
        cena = r[5] or r[15] or r[8]
        cena_m2 = round(cena / pow_m2, 2) if (cena and pow_m2 and pow_m2 > 0) else None
        features.append(compact({
            "lon": round(lon, 5), "lat": round(lat, 5),
            "id_transakcji": r[0],
            "rodzaj_transakcji": lookup(RODZAJ_TRANSAKCJI, r[1]),
            "rynek": lookup(RODZAJ_RYNKU, r[2]),
            "sprzedajacy": lookup(STRONA, r[3]),
            "kupujacy": lookup(STRONA, r[4]),
            "cena_transakcji": r[5], "vat_transakcji": r[6],
            "rodzaj_nieruchomosci": lookup(RODZAJ_NIERUCHOMOSCI, r[7]),
            "cena_nieruchomosci": r[8],
            "prawo_do_nieruchomosci": lookup(RODZAJ_PRAWA, r[9]),
            "udzial_w_prawie": r[10],
            "pow_nieruchomosci_gruntowej_ha": r[11],
            "opis_nieruchomosci": r[12],
            "id_budynku": r[13], "pow_uzytkowa_budynku_m2": r[14],
            "cena_budynku": r[15],
            "rodzaj_budynku": lookup(RODZAJ_BUDYNKU, r[16]),
            "dodatkowe_info_budynek": r[17], "vat_budynek": r[18],
            "cena_za_m2": cena_m2,
            "oznaczenie_dokumentu": r[19],
            "data_dokumentu": r[20], "notariusz": r[21],
            "miejscowosc": r[22], "ulica": r[23], "numer": r[24],
        }))
    print(f"    {len(features):,} building transactions")
    return features


def write_gz_json(data, path):
    raw = json.dumps(data, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    with gzip.open(path, 'wb', compresslevel=9) as f:
        f.write(raw)
    print(f"    {path.name}: {len(raw)/1e6:.1f} MB -> {path.stat().st_size/1e6:.1f} MB")


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <input.gpkg> <output_dir>")
        sys.exit(1)

    gpkg_path = Path(sys.argv[1])
    out_dir = Path(sys.argv[2])
    data_dir = out_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(gpkg_path))
    print("Exporting transactions with full properties...")
    lokale = export_lokale(conn)
    dzialki = export_dzialki(conn)
    budynki = export_budynki(conn)
    conn.close()

    print("\n  Writing gzipped JSON...")
    write_gz_json(lokale, data_dir / "lokale.json.gz")
    write_gz_json(dzialki, data_dir / "dzialki.json.gz")
    write_gz_json(budynki, data_dir / "budynki.json.gz")

    # Copy the HTML template
    html_src = Path(__file__).parent / "index.html"
    html_dst = out_dir / "index.html"
    html_dst.write_text(html_src.read_text(encoding='utf-8'), encoding='utf-8')

    print(f"\nDone! Output in {out_dir}/")
    print(f"  index.html + data/lokale.json.gz + data/dzialki.json.gz + data/budynki.json.gz")


if __name__ == "__main__":
    main()

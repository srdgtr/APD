# voorraad file changes multiple times a day on server

import os
import pandas as pd
import numpy as np
from datetime import datetime
import dropbox
import requests
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
import configparser
from pathlib import Path
import sys

sys.path.insert(0, str(Path.cwd().parent))
from bol_export_file import get_file
from process_results.process_data import save_to_db, save_to_dropbox

ini_config = configparser.ConfigParser(interpolation=None)
ini_config.read(Path.home() / "bol_export_files.ini")
config_db = dict(
    drivername="mariadb",
    username=ini_config.get("database odin", "user"),
    password=ini_config.get("database odin", "password"),
    host=ini_config.get("database odin", "host"),
    port=ini_config.get("database odin", "port"),
    database=ini_config.get("database odin", "database"),
)
engine = create_engine(URL.create(**config_db))
scraper_name = Path.cwd().name
korting_percent = int(ini_config.get("stap 1 vaste korting", scraper_name.lower()).strip("%"))

voorraad_file = requests.get(ini_config.get("beekman", "voorraad_url")).content

with open("beekman.csv", "wb") as f:
    f.write(voorraad_file)

voorraad = (
    pd.read_csv(
        max(Path.cwd().glob("beekman*.csv"), key=os.path.getctime),
        sep=";",
        dtype={
            "EAN barcode": object,
            "Aantal verkoop eenheden": object,
            "Inkoop prijs excl. btw": float,
            "Voorraad Ja/Nee": object,
            "Artikel nr": object,
        },
        low_memory=False,
    )
    .rename(columns=lambda x: x.replace(".", ""))  # strip point from colum names
    .rename(
        columns={
            "Artikel nr": "sku",
            "Merk": "brand",
            "Origineel nr": "id",
            "Groep": "group",
            "Consumenten prijs incl btw": "price_going",
            "Inkoop prijs excl btw": "price",
        }
    )
    .assign(
        info=lambda x: x["Omschrijving"].str.cat(x[["Kenmerk", "Type", "Verpakking"]], sep=" ", na_rep=""),
        stock=lambda x: np.where(
            x["Staffel 2 aantal"].isnull(),
            0,
            x.iloc[:, x.columns.str.contains("Staffel") & x.columns.str.contains("aantal")]
            .fillna(0)
            .max(1)
            .astype(int)
            .multiply(3),
        ),  # waneer staffel mutiply maar niet waneer staffel 2 leeg is
    )
    .assign(
        aantal_verkoop_eenheden=lambda x: x["Aantal verkoop eenheden"].fillna(0).astype(int).multiply(5),
        stock=lambda x: np.where(x["stock"] == 0, x["aantal_verkoop_eenheden"], x["stock"]),
        eigen_sku=lambda x: scraper_name + x["sku"],
        ean=lambda x: pd.to_numeric(x["EAN barcode"].fillna(x["EAN_extra_1"]), errors="coerce")
        .astype("Int64")
        .fillna(0),
        gewicht="",
        lange_omschrijving="",
        verpakings_eenheid="",
        lk=lambda x: (korting_percent * x["price"] / 100).round(2),
        price=lambda x: (x["price"] - x["lk"]).round(2),
        group=lambda x: x.group.fillna(""),
    )
    .assign(
        stock=lambda x: np.where(
            x[" Voorraad Ja/Nee"].str.contains("N"), 0, x["stock"]
        ),  # als voorraad nee geeft is er geen natuurlijk geen voorraad
    )
    .assign(
        stock=lambda x: np.where(x["Inactief code"].str.contains("NIETLEVERBAAR"), 0, x["stock"]),
    )
    .query("stock > 0")
    # .query("ean > 10000000") Ik zie ook zonder ean verkocht worden
    .replace(
        {
            "group": {
                "Wasmachine": "Accessoires wassen & drogen",
                "Vaatwasser": "Accessoires vaatwassers",
                "Koelkast": "Accessoires koelkasten",
                "Wasdroger": "Accessoires wassen & drogen",
            }
        }
    )
)


basis_voorraad = voorraad[["sku", "ean", "brand", "id", "group", "info", "stock", "price", "price_going", "lk"]]

date_now = datetime.now().strftime("%c").replace(":", "-")

basis_voorraad.to_csv(f"{scraper_name}_{date_now}.csv", index=False, encoding="utf-8-sig")

get_orgineel_numbers = pd.read_csv(max(Path.cwd().glob("beekman*.csv"), key=os.path.getctime),sep=";", low_memory=False,usecols=["EAN barcode","EAN_extra_1","Origineel nr"]).rename(columns={"Origineel nr":'origineel_nr'})

normale_ean = get_orgineel_numbers[['origineel_nr', 'EAN barcode']].rename(columns={'EAN barcode': 'ean'})
extra_ean = get_orgineel_numbers[['origineel_nr', 'EAN_extra_1']].rename(columns={'EAN_extra_1': 'ean'})
orgineelnummers = pd.concat([normale_ean, extra_ean]).dropna(subset=['ean']).assign(ean=lambda x: pd.to_numeric(x["ean"].str.replace(r"[^\d]", "", regex=True)).astype(int)).set_index('ean')
orgineelnummers.to_sql(name="orgineelnummers", con=engine, if_exists="replace")

os.remove("beekman.csv")

latest_file = max(Path.cwd().glob(f"{scraper_name}_*.csv"), key=os.path.getctime)
save_to_dropbox(latest_file, scraper_name)

basis_voorraad[['sku', 'price']].rename(columns={'price': 'Inkoopprijs exclusief'}).to_csv(f"{scraper_name}_Vendit_price_kaal.csv", index=False, encoding="utf-8-sig")

product_info = basis_voorraad.rename(
    columns={
        # "ean":"ean",
        "brand": "merk",
        "stock": "voorraad",
        "price": "inkoop_prijs",
        # :"promo_inkoop_prijs",
        # :"promo_inkoop_actief",
        # "": "advies_prijs",
        "group" :"category",
        "info": "omschrijving",
    }
).assign(onze_sku=lambda x: scraper_name + x["sku"], import_date=datetime.now())

save_to_db(product_info)


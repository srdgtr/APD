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

sys.path.insert(0, str(Path.home()))
from bol_export_file import get_file

dbx = dropbox.Dropbox(os.environ.get("DROPBOX"))
ini_config = configparser.ConfigParser(interpolation=None)
ini_config.read(Path.home() / "bol_export_files.ini")
config_db = dict(
    drivername="mariadb",
    username=ini_config.get("database leveranciers", "user"),
    password=ini_config.get("database leveranciers", "password"),
    host=ini_config.get("database leveranciers", "host"),
    port=ini_config.get("database leveranciers", "port"),
    database=ini_config.get("database leveranciers", "database"),
)
config_db_odin = dict(
    drivername="mariadb",
    username=ini_config.get("database odin", "user"),
    password=ini_config.get("database odin", "password"),
    host=ini_config.get("database odin", "host"),
    port=ini_config.get("database odin", "port"),
    database=ini_config.get("database odin", "database"),
)
engine = create_engine(URL.create(**config_db))
odin_db = create_engine(URL.create(**config_db_odin))
current_folder = Path.cwd().name.upper()
korting_percent = int(ini_config.get("stap 1 vaste korting", current_folder.lower()).strip("%"))

beekman_file = requests.get(ini_config.get("beekman", "voorraad_url")).content

with open("beekman.csv", "wb") as f:
    f.write(beekman_file)

beekman = (
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
        eigen_sku=lambda x: "APD" + x["sku"],
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


beekman_basis = beekman[["sku", "ean", "brand", "id", "group", "info", "stock", "price", "price_going", "lk"]]

date_now = datetime.now().strftime("%c").replace(":", "-")

beekman_basis.to_csv("APD_" + date_now + ".csv", index=False, encoding="utf-8-sig")

os.remove("beekman.csv")

latest_file = max(Path.cwd().glob("APD_*.csv"), key=os.path.getctime)
with open(latest_file, "rb") as f:
    dbx.files_upload(
        f.read(), "/macro/datafiles/APD/" + latest_file.name, mode=dropbox.files.WriteMode("overwrite", None), mute=True
    )

apd_info = beekman.rename(
    columns={
        "brand": "merk",
        "group": "category",
        "info": "product_omschrijving",
        "price": "prijs",
        "price_going": "advies_prijs",
        "stock": "voorraad",
    }
)

beekman_orgineelnummers = beekman[["ean", "id"]]

beekman_orgineelnummers.to_sql(name="orgineelnummers", con=odin_db, if_exists="replace",index=False)

odin_db.dispose()
engine.dispose()

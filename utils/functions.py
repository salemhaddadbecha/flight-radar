from pyspark.sql.functions import col, count, when
from pathlib import Path
from datetime import datetime
from extract.extract_airports import get_continent_from_airport_iata

def get_latest_csv_file(base_path: str) -> str:
    today = datetime.today()
    path = Path(base_path) / \
           f"tech_year={today.year}" / \
           f"tech_month={today.strftime('%Y-%m')}" / \
           f"tech_day={today.strftime('%Y-%m-%d')}"

    if not path.exists():
        raise FileNotFoundError(f"Le dossier du jour n'existe pas: {path.as_posix()}")

    files = sorted([f for f in path.glob("*.csv")], reverse=True)
    if not files:
        raise FileNotFoundError(f"Aucun fichier CSV trouvé dans : {path.as_posix()}")

    return str(files[0])  # ou files[0].as_posix() si tu veux garder les slashes

# Fonction pour extraire le constructeur depuis le modèle d'avion
def get_constructor(model: str) -> str:
    model = model.lower()
    if "airbus" in model:
        return "Airbus"
    elif "boeing" in model:
        return "Boeing"
    elif "embraer" in model:
        return "Embraer"
    elif "bombardier" in model:
        return "Bombardier"
    elif "atr" in model:
        return "ATR"
    elif "cessna" in model:
        return "Cessna"
    elif "tecnam" in model:
        return "Tecnam"
    else:
        return "Autre"

def get_flight_duration(departure: int, arrival: int) -> float:
    if departure is None:
        return None
    if arrival is None:
        return None
    duration_seconds = arrival - departure
    return duration_seconds / 3600.0


def data_cleaning(df):
    # Get count of non null values per column
    non_null_counts = df.select([
        count(when(col(c).isNotNull() & (col(c) != ""), c)).alias(c)
        for c in df.columns
    ])

    non_null_dict = non_null_counts.collect()[0].asDict()

    columns_to_keep = [col for col, count in non_null_dict.items() if count > 0]

    return df.select(columns_to_keep)



def load_airport():
    get_continent_from_airport_iata()
    return spark.read.option("header", True).csv("data/ref/airports_continents.csv")
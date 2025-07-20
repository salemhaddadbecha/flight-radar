import os
import pandas as pd
import logging
from datetime import datetime
import pytz
from FlightRadar24  import FlightRadar24API
os.makedirs("logs", exist_ok=True)
now = datetime.now(pytz.timezone("Europe/Paris"))
# Configurer le logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(f"logs/extract_flights_{now.strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def extract_flights():
    logger.info("Début de l'extraction des vols.")
    try:
        api = FlightRadar24API()
        flights = api.get_flights()
        logger.info(f"{len(flights)} vols récupérés depuis l'API.")

        records = [vars(flight) for flight in flights]

        now = datetime.now(pytz.timezone("Europe/Paris"))
        folder = f"data/Flights/rawzone/tech_year={now.year}/tech_month={now.strftime('%Y-%m')}/tech_day={now.strftime('%Y-%m-%d')}"
        os.makedirs(folder, exist_ok=True)
        logger.info(f"Dossier créé : {folder}")

        filename = f"{folder}/flights_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        pd.DataFrame(records).to_csv(filename, index=False)
        logger.info(f"Fichier sauvegardé : {filename}")

    except Exception as e:
        logger.error(f"Erreur lors de l'extraction des vols : {e}", exc_info=True)




if __name__ == "__main__":
    extract_flights()

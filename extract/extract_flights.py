import time
import os
import pandas as pd
import logging
import pytz
from FlightRadar24  import FlightRadar24API
from datetime import datetime

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


def extract_flights_details():
    logger.info("Début de l'extraction des vols.")
    try:
        api = FlightRadar24API()
        flights = api.get_flights()
        logger.info(f"{len(flights)} vols récupérés depuis l'API.")

        enriched_records = []

        for flight in flights[:10]:
            try:
                details = api.get_flight_details(flight)
                record = vars(flight)

                # Extraire les timestamps réels ou estimés
                time_info = details.get("time", {})
                real_dep = time_info.get("real", {}).get("departure")
                real_arr = time_info.get("real", {}).get("arrival")
                est_arr = time_info.get("estimated", {}).get("arrival")

                record["departure_time"] = real_dep
                record["arrival_time"] = real_arr if real_arr else est_arr

                enriched_records.append(record)
                time.sleep(0.5)
            except Exception as e:
                logger.warning(f"Erreur en récupérant les détails pour un vol : {e}")

        now = datetime.now(pytz.timezone("Europe/Paris"))
        folder = f"data/Flights/rawzone/tech_year={now.year}/tech_month={now.strftime('%Y-%m')}/tech_day={now.strftime('%Y-%m-%d')}"
        os.makedirs(folder, exist_ok=True)

        filename = f"{folder}/flights_details_{now.strftime('%Y%m%d_%H%M%S')}.csv"
        pd.DataFrame(enriched_records).to_csv(filename, index=False)
        logger.info(f"Fichier sauvegardé : {filename}")

    except Exception as e:
        logger.error(f"Erreur lors de l'extraction des vols : {e}", exc_info=True)


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






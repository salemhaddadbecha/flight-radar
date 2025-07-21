import logging
from utils.functions import get_constructor
from collections import defaultdict, Counter
import time
from FlightRadar24  import FlightRadar24API
from datetime import datetime
import pytz


logger = logging.getLogger(__name__)
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

def get_active_company():
    """KPI5"""
    logger.info("Début de l'extraction des vols.")
    try:
        api = FlightRadar24API()
        flights = api.get_flights()
        logger.info(f"{len(flights)} vols récupérés depuis l'API.")

        constructor_count = Counter()
        for i, flight in enumerate(flights[400:450]):
            try:
                details = api.get_flight_details(flight)

                model_info = details.get("aircraft", {}).get("model", {}).get("text", "")
                print(model_info)
                constructor = get_constructor(model_info)
                constructor_count[constructor] += 1
            except Exception as e:
                logger.warning(f"Erreur sur le vol {flight.id}: {e}")
            time.sleep(0.5)
            # Affichage
        logger.info("Nombre de vols actifs par constructeur :")
        for k, v in constructor_count.most_common():
            print(f"{k}: {v} vols actifs")
        print(constructor_count)
        # KPI2 : constructeur avec le + de vols actifs
        top_constructor = constructor_count.most_common(1)[0]
        print(f"\n Constructeur avec le plus de vols actifs : {top_constructor[0]} ({top_constructor[1]} vols)")

    except Exception as e:
        logger.error(f"Erreur lors de l'extraction des vols : {e}", exc_info=True)


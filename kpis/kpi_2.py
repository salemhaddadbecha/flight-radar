from pyspark.sql.functions import col, count, desc, row_number
from pyspark.sql.window import Window
from configs.spark_config import spark
from datetime import datetime
import pytz
from extract.extract_airlines import extract_airlines
from utils.functions import load_airport, data_cleaning
import logging

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

def process_flights_by_continent(input_path):
    """KPI2 - Top compagnie aérienne par continent (vols intra-continent)"""
    try:
        flights = spark.read.option("header", True).csv(input_path)
        flights = data_cleaning(flights)

        # Mapping des aéroports -> continents
        airport_continents = load_airport()
        logger.info("Mapping aéroports/continents effectué.")

        flights = (
            flights
            .join(airport_continents.withColumnRenamed("continent", "continent_origin"),
                  flights["origin_airport_iata"] == airport_continents["iata"], how="left")
            .drop("iata")
            .join(airport_continents.withColumnRenamed("continent", "continent_destination"),
                  flights["destination_airport_iata"] == airport_continents["iata"], how="left")
            .drop("iata")
        )

        # Garder les vols régionaux (même continent)
        regional_flights = flights.filter(
            col("continent_origin").isNotNull() &
            (col("continent_origin") == col("continent_destination"))
        )

        # Charger les compagnies
        airlines = extract_airlines()

        # Joindre avec les compagnies
        joined_df = regional_flights.join(
            airlines,
            regional_flights["airline_iata"] == airlines["IATA"],
            how="left"
        )

        # Calcul du top par continent
        continent_company_counts = (
            joined_df.groupBy("continent_origin", "name")
            .agg(count("*").alias("nb_vols"))
        )

        window_spec = Window.partitionBy("continent_origin").orderBy(desc("nb_vols"))

        top_company_per_continent = (
            continent_company_counts.withColumn("rank", row_number().over(window_spec))
            .filter((col("rank") == 1) & col("name").isNotNull())
            .drop("rank")
        )
        top_company_per_continent.coalesce(1).write.mode("overwrite").option("header", True).csv("data/indicators/kpi_2_company_continent_vols")
        logger.info("KPI 2 calculé avec succès.")

    except Exception as e:
        logger.error(f"Erreur lors du processing : {e}", exc_info=True)
    finally:
        spark.stop()

import time
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, count, desc, when, udf, row_number
from pyspark.sql.window import Window
from configs.spark_config import spark
from datetime import datetime
import pytz
from extract.extract_airlines import extract_airlines
from utils.functions import  get_latest_csv_file
from extract.extract_flights import extract_flights
from extract.extract_airports import get_continent_from_airport_iata
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

def data_cleaning(df):
    # Get count of non null values per column
    non_null_counts = df.select([
        count(when(col(c).isNotNull() & (col(c) != ""), c)).alias(c)
        for c in df.columns
    ])

    non_null_dict = non_null_counts.collect()[0].asDict()

    columns_to_keep = [col for col, count in non_null_dict.items() if count > 0]

    return df.select(columns_to_keep)


def calculate_top_active_flight_company(input_path):
    """
    KPI_1: La compagnie avec le + de vols en cours
    """
    flights = spark.read.option("header", True).csv(input_path)
    flights = data_cleaning(flights)
    flights_cleaned = flights.filter(
        (col("airline_iata").isNotNull()) &
        (col("on_ground") == "0")  # 1 au sol; 0 en vol.

    )

    airlines = extract_airlines()

    joined_df = flights_cleaned.join(
        airlines,
        flights_cleaned["airline_iata"] == airlines["IATA"],
        how="left"
    )

    top_company  = (
        joined_df.groupBy("name")
        .agg(count("*").alias("nb_vols"))
        .orderBy(desc("nb_vols")).limit(1)
    )

    top_company.coalesce(1).write.mode("overwrite").option("header", True).csv("data/indicators/kpi_1_company_vols")
    logger.info(f"KPI 1 calculé")
    spark.stop()



def build_airport_continent_mapping():
    get_continent_from_airport_iata()
    return spark.read.option("header", True).csv("data/ref/airports_continents.csv")

def process_flights_by_continent(input_path):
    """KPI2 - Top compagnie aérienne par continent (vols intra-continent)"""
    try:
        flights = spark.read.option("header", True).csv(input_path)
        flights = data_cleaning(flights)

        # Mapping des aéroports -> continents
        airport_continents = build_airport_continent_mapping()
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

if __name__ == "__main__":
    extract_flights()
    base_path = "C:/Users/salem/PycharmProjects/haddad-salem-flight-radar-bb3eeff6-a6c0-4e44-9cc8-68bc2d2189f3/data/Flights/rawzone"
    input_path = get_latest_csv_file(base_path)
    #calculate_top_active_flight_company(input_path)
    #process_flights_by_continent(input_path)

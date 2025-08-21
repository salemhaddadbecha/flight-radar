from pyspark.sql.functions import col, count, desc, when, row_number, radians, sin, cos, sqrt, asin, udf, avg
from pyspark.sql.types import FloatType
from pyspark.sql.window import Window
from configs.spark_config import spark
from datetime import datetime
import pytz
from extract.extract_airlines import extract_airlines
from utils.functions import  get_latest_csv_file, data_cleaning
from extract.extract_flights import extract_flights
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


if __name__ == "__main__":
    extract_flights()
    base_path = "C:/Users/salem/PycharmProjects/haddad-salem-flight-radar-bb3eeff6-a6c0-4e44-9cc8-68bc2d2189f3/data/Flights/rawzone"
    input_path = get_latest_csv_file(base_path)
    #calculate_top_active_flight_company(input_path)
    #process_flights_by_continent(input_path)
    #top_flight_distance(input_path)
    #extract_flights_details()
    #compute_flight_duration(input_path)


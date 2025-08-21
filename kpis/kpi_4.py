from pyspark.sql.functions import col, udf, avg
from pyspark.sql.types import FloatType
from configs.spark_config import spark
from utils.functions import   get_flight_duration, data_cleaning
import logging
import datetime
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
def compute_flight_duration(input_path):
    """KPI 4"""
    try:
        duration_udf = udf(get_flight_duration, FloatType())
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
        flights_with_duration = flights.withColumn(
            "flight_duration_hours",
            duration_udf(col("departure_time"), col("arrival_time"))
        )

        # Garder les vols régionaux (même continent)
        same_continent_flights  = flights_with_duration.filter(
            col("continent_origin").isNotNull() &
            (col("continent_origin") == col("continent_destination"))
        )
        avg_duration_per_continent = same_continent_flights.groupBy("continent_origin").agg(avg("flight_duration_hours").alias("avg_flight_duration_hours"))
        avg_duration_per_continent.show(truncate=False)
    except Exception as e:
        logger.error(f"Erreur lors du processing : {e}", exc_info=True)
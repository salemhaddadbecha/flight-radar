from pyspark.sql.functions import col, radians, sin, cos, sqrt, asin, udf, avg

from configs.spark_config import spark
from utils.functions import  load_airport, data_cleaning
from extract.extract_flights import extract_flights
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
def top_flight_distance(input_path):
    """KPI3 - Le vol en cours avec le trajet le plus long"""
    try:
        flights = spark.read.option("header", True).csv(input_path)
        flights = data_cleaning(flights)
        flights_cleaned = flights.filter(
            (col("airline_iata").isNotNull()) &
            (col("on_ground") == "0")  # 1 au sol; 0 en vol.

        )
        # Mapping des aéroports -> continents
        airports_coords = load_airport()

        flights_with_coords = (
            flights_cleaned
            .join(airports_coords.withColumnRenamed("IATA", "origin_airport_iata")
                  .withColumnRenamed("Latitude", "origin_lat")
                  .withColumnRenamed("Longitude", "origin_lon")
                  .withColumnRenamed("Continent", "orign_continent"),
                  on="origin_airport_iata", how="left")
            .join(airports_coords.withColumnRenamed("IATA", "destination_airport_iata")
                  .withColumnRenamed("Latitude", "dest_lat")
                  .withColumnRenamed("Longitude", "dest_lon")
                  .withColumnRenamed("Continent", "dest_continent"),
                  on="destination_airport_iata", how="left")
        )
        R = 6371.0

        flights_with_distance = flights_with_coords.withColumn("distance_km",
                                                               2 * R * asin(sqrt(
                                                                   sin((radians(col("dest_lat")) - radians(
                                                                       col("origin_lat"))) / 2) ** 2 +
                                                                   cos(radians(col("origin_lat"))) * cos(
                                                                       radians(col("dest_lat"))) *
                                                                   sin((radians(col("dest_lon")) - radians(
                                                                       col("origin_lon"))) / 2) ** 2
                                                               ))
                                                               )

        # 4. Trouver le vol en cours avec la plus grande distance
        longest_flight = flights_with_distance.orderBy(col("distance_km").desc()).limit(1)
        longest_flight.show(truncate=False)
        logger.info("KPI 3 calculé avec succès.")
    except Exception as e:
        logger.error(f"Erreur lors du processing : {e}", exc_info=True)
    finally:
        spark.stop()
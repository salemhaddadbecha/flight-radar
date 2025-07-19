from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc

def process_flights(input_path: str):
    spark = SparkSession.builder.appName("ProcessFlightData").getOrCreate()
    df = spark.read.option("header", True).csv(input_path)

    df_clean = df.filter(
        (col("airline_iata").isNotNull()) &
        (col("on_ground") == "0") &
        (col("aircraft_code").isNotNull())
    )

    # Exemple d’indicateur : compagnie avec le + de vols
    company_count = (
        df_clean.groupBy("airline_iata")
        .agg(count("*").alias("nb_vols"))
        .orderBy(desc("nb_vols"))
    )

    company_count.show(10, truncate=False)

    # Tu peux sauvegarder les résultats ou retourner le DataFrame
    spark.stop()

if __name__ == "__main__":
    import sys
    input_path =   "C:/Users/salem/PycharmProjects/haddad-salem-flight-radar-bb3eeff6-a6c0-4e44-9cc8-68bc2d2189f3/data/Flights/rawzone/tech_year=2025/tech_month=2025-07/tech_day=2025-07-19/flights_20250719_084941.csv"  #sys.argv[1]  # Exemple : data/Flights/rawzone/.../flights_*.csv
    process_flights(input_path)


from FlightRadar24  import FlightRadar24API
from configs.spark_config import spark


def extract_airlines():
    fr_api = FlightRadar24API()
    airlines_data = fr_api.get_airlines()
    return  spark.createDataFrame(airlines_data)


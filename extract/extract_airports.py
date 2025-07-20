import pandas as pd
from utils.continents import get_continent

def get_continent_from_airport_iata():
    columns = [
        "Airport_ID", "Name", "City", "Country", "IATA", "ICAO",
        "Latitude", "Longitude", "Altitude", "Timezone", "DST",
        "Tz_database_time_zone", "Type", "Source"
    ]

    airports_df = pd.read_csv(
        "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat",
        header=None,
        names=columns
    )
    airports_df = airports_df[airports_df["IATA"] != r'\N']
    airports_df["Continent"] = airports_df["Country"].apply(get_continent)
    airports_df = airports_df[["IATA", "Continent"]].dropna()
    airports_df.to_csv("data/ref/airports_continents.csv", index=False)




if __name__ == "__main__":
    get_continent_from_airport_iata()





import os
import pandas as pd
from datetime import datetime
from FlightRadar24 import FlightRadar24API
import logging

def extract_flights():
    api = FlightRadar24API()
    flights = api.get_flights()
    records = [vars(flight) for flight in flights]

    now = datetime.utcnow()
    folder = f"data/Flights/rawzone/tech_year={now.year}/tech_month={now.strftime('%Y-%m')}/tech_day={now.strftime('%Y-%m-%d')}"
    os.makedirs(folder, exist_ok=True)

    filename = f"{folder}/flights_{now.strftime('%Y%m%d_%H%M%S')}.csv"
    pd.DataFrame(records).to_csv(filename, index=False)
    #logger.info(f"Fichier sauvegardé : {filename}")

if __name__ == "__main__":
    extract_flights()

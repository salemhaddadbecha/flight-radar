from collections import defaultdict, Counter
from FlightRadar24  import FlightRadar24API

def get_airline_country_map(api):
    airlines = api.get_airlines()
    mapping = {}
    for airline in airlines:
        # Ex: airline.code = {'iata': 'EK', 'icao': 'UAE'}
        iata = airline.get("code", {}).get("iata")
        icao = airline.get("code", {}).get("icao")
        country = airline.get("country", "Inconnu")
        if iata:
            mapping[iata] = country
        if icao:
            mapping[icao] = country
    return mapping

def get_top_models_by_country():
    api = FlightRadar24API()
    flights = api.get_flights()

    airline_country_map = get_airline_country_map(api)

    # {pays: Counter({modele: count})}
    country_model_counter = defaultdict(Counter)

    for flight in flights[:100]:  # limite à 100 pour test
        try:
            details = api.get_flight_details(flight)
            airline_info = details.get("airline", {})
            aircraft_info = details.get("aircraft", {})
            airport_info = details.get("airport", {})

            # Récupérer le code iata ou icao de la compagnie
            airline_code = airline_info.get("code", {}).get("iata") or airline_info.get("code", {}).get("icao")

            # Trouver le pays via mapping
            country = airline_country_map.get(airline_code)

            # Si pas trouvé, fallback sur pays de l'aéroport d'origine
            if not country:
                country = airport_info.get("origin", {}).get("position", {}).get("country", {}).get("name", "Inconnu")

            model = aircraft_info.get("model", {}).get("text", "Inconnu")

            if country != "Inconnu" and model != "Inconnu":
                country_model_counter[country][model] += 1

        except Exception as e:
            print(f"Erreur sur le vol {flight.id}: {e}")

    # Affichage top 3 modèles par pays
    for country, model_counts in country_model_counter.items():
        print(f"\nPays: {country}")
        for model, count in model_counts.most_common(3):
            print(f"  {model}: {count} vols actifs")
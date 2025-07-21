# FlightRadar24 - KPI Pipeline

Ce projet calcule plusieurs KPIs à partir des données de vol (FlightRadar24) en utilisant **Apache Airflow** pour l'orchestration et **Spark/Python** pour le traitement.

## KPIs calculés

| KPI | Description |
|-----|-------------|
| 1   | Nombre de vols actifs par compagnie aérienne |
| 2   | Nombre de vols par continent |
| 3   | Vol actif avec la distance la plus longue |
| 8   | Modèle d’avion le plus utilisé en vol actuellement |
| 12  | Distance maximale par pays d'origine |
| 16  | Top 3 des modèles d’avions en usage par pays de compagnie aérienne (via API FlightRadar24) |

 
## Configuration Airflow

1. **Placer le dossier `airflow/` dans votre environnement Airflow.**
2. Copier les données CSV dans `data/Flights/`.
3. Le DAG est exécuté automatiquement tous les jours (`@daily`).
4. Les résultats sont exportés dans `data/indicators/`.

## Dépendances

- Apache Airflow
- Apache Spark (local ou cluster)
- FlightRadar24 API (Python SDK)
- Pandas

## 🚀 Lancer manuellement un DAG

```bash
airflow dags trigger kpi_flight_dag

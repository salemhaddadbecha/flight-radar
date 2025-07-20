from pathlib import Path
from datetime import datetime

def get_latest_csv_file(base_path: str) -> str:
    today = datetime.today()
    path = Path(base_path) / \
           f"tech_year={today.year}" / \
           f"tech_month={today.strftime('%Y-%m')}" / \
           f"tech_day={today.strftime('%Y-%m-%d')}"

    if not path.exists():
        raise FileNotFoundError(f"Le dossier du jour n'existe pas: {path.as_posix()}")

    files = sorted([f for f in path.glob("*.csv")], reverse=True)
    if not files:
        raise FileNotFoundError(f"Aucun fichier CSV trouvé dans : {path.as_posix()}")

    return str(files[0])  # ou files[0].as_posix() si tu veux garder les slashes

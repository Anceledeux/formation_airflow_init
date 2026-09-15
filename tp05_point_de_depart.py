from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task

DATA_DIR = Path("/usr/local/airflow/include/data")

default_args = {
    "owner": "formation",
}


@dag(
    dag_id="snowystore_etl",
    description="Fil rouge Snowystore : logique metier encore dans le DAG",
    start_date=datetime(2026, 9, 4),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["snowystore", "tp5"],
)
def snowystore_etl():

    @task
    def extract(**context) -> list[dict]:
        """Lit le CSV du jour et renvoie les lignes brutes."""
        ds = context["ds"]
        chemin = DATA_DIR / f"orders_{ds}.csv"
        if not chemin.exists():
            raise FileNotFoundError(
                f"{chemin} introuvable. "
                "Generer les donnees : python include/generate_orders.py --days 20"
            )
        with chemin.open(encoding="utf-8") as fh:
            lignes = list(csv.DictReader(fh))
        print(f"{len(lignes)} lignes brutes lues pour le {ds}")
        return lignes

    @task
    def transform(lignes: list[dict]) -> dict[str, float]:
        """Nettoie puis agrege. TOUTE la logique est en dur ici : c'est le probleme."""
        # Regles metier : montant numerique et strictement positif,
        # categorie non vide, normalisee en minuscules sans espaces parasites.
        nettoyees = []
        for ligne in lignes:
            try:
                montant = float(ligne.get("amount", 0))
            except (TypeError, ValueError):
                continue                      # montant illisible -> on ecarte
            if montant <= 0:
                continue                      # montant nul ou negatif -> on ecarte
            categorie = str(ligne.get("category", "")).strip().lower()
            if not categorie:
                continue                      # categorie manquante -> on ecarte
            nettoyees.append({**ligne, "amount": montant, "category": categorie})

        rejetees = len(lignes) - len(nettoyees)
        print(f"{len(nettoyees)} lignes conservees, {rejetees} rejetees")

        totaux: dict[str, float] = {}
        for ligne in nettoyees:
            categorie = ligne["category"]
            totaux[categorie] = round(totaux.get(categorie, 0.0) + ligne["amount"], 2)

        for categorie, total in sorted(totaux.items()):
            print(f"  {categorie:14} : {total}")
        return totaux

    @task
    def load(totaux: dict[str, float], **context) -> None:
        """Controle qualite puis ecriture du resultat."""

        anomalies = []
        for categorie, total in totaux.items():
            if total < 0:
                anomalies.append(f"total negatif pour '{categorie}' : {total}")
        if not totaux:
            anomalies.append("aucune donnee agregee")

        if anomalies:
            raise ValueError("Controle qualite en echec : " + " | ".join(anomalies))

        sortie = DATA_DIR / "out"
        sortie.mkdir(parents=True, exist_ok=True)
        fichier = sortie / f"sales_{context['ds']}.json"
        fichier.write_text(json.dumps(totaux, indent=2), encoding="utf-8")
        print(f"Resultat ecrit dans {fichier}")

    load(transform(extract()))


snowystore_etl()

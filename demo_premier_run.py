"""DEMO — lire un run dans l'interface (tout premier exercice, Jour 1).

Ce DAG n'a qu'un but : donner quelque chose a REGARDER dans l'interface.
Il ne dévoile aucun exercice — le TP1 reste entier.

Il est concu pour que chaque vue de l'UI montre quelque chose d'interessant :
  - vue Graph : une fourche (2 taches en parallele) puis une jonction
  - vue Grid  : des durees nettement differentes d'une tache a l'autre
  - Logs      : des messages explicites, faciles a retrouver
  - XCom      : une valeur transmise d'une tache a l'autre

MODE D'EMPLOI (stagiaire)
------------------------
1. Copier ce fichier dans dags/
2. Attendre ~30 s qu'il apparaisse dans la liste
3. ACTIVER l'interrupteur a gauche du nom, PUIS declencher
4. Explorer : Grid, Graph, Logs d'une tache, onglet XCom

Un DAG est EN PAUSE par defaut : si vous le declenchez sans activer
l'interrupteur, les taches resteront en attente sans message d'erreur.
"""
from __future__ import annotations

import time
from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="demo_premier_run",
    start_date=datetime(2026, 1, 1),
    schedule=None,               # demonstration : declenchement manuel uniquement
    catchup=False,
    tags=["demo", "jour1"],
    doc_md=__doc__,              # cette doc s'affiche dans l'onglet "Docs" du DAG
)
def demo_premier_run():

    @task
    def preparer() -> int:
        """Tache d'ouverture : elle RENVOIE une valeur (donc un XCom)."""
        print("Preparation des donnees Snowystore...")
        nb_commandes = 1247
        print(f"{nb_commandes} commandes a traiter")
        return nb_commandes

    @task
    def compter_articles(nb: int) -> int:
        """Branche 1 : rapide."""
        print(f"Comptage des articles sur {nb} commandes")
        return nb * 3

    @task
    def calculer_chiffre_affaires(nb: int) -> float:
        """Branche 2 : volontairement plus lente, pour voir la difference
        de duree dans la vue Grid et dans le Gantt."""
        print("Calcul du chiffre d'affaires (traitement long)...")
        time.sleep(8)
        montant = round(nb * 87.5, 2)
        print(f"Chiffre d'affaires : {montant} EUR")
        return montant

    @task
    def publier(articles: int, montant: float) -> None:
        """Jonction : elle attend LES DEUX branches."""
        print("=" * 46)
        print(f"  Articles vendus      : {articles}")
        print(f"  Chiffre d'affaires   : {montant} EUR")
        print("=" * 46)
        print("Rapport publie. Cherchez ce message dans les logs !")

    nb = preparer()
    publier(compter_articles(nb), calculer_chiffre_affaires(nb))


demo_premier_run()

"""TP7 — Branching, TaskGroups & mapping dynamique, DEPUIS LA VERSION TASKFLOW DU TP1.

On reprend dags/snowystore_etl.py (version TaskFlow) et on ajoute les trois
mecanismes avances, dans CE MEME FICHIER.

CE QUI CHANGE PAR RAPPORT AU TP4
---------------------------------
1. Un branchement : @task.branch choisit la strategie de chargement
2. Un TaskGroup : les transformations sont regroupees visuellement
3. Un mapping dynamique : une tache PAR CATEGORIE, nombre connu a l'EXECUTION
4. Une jointure : sans trigger_rule adaptee, elle se fait skipper en cascade

LES DEUX PIEGES DU TP
---------------------
* Les branches non retenues passent en "skipped", PAS en "failed". Une tache aval
  en all_success (la regle par defaut) se fait donc skipper EN CASCADE.
  -> correction : trigger_rule="none_failed_min_one_success"

* Dans un TaskGroup, les task_id sont PREFIXES : "transformations.transform_category".
  Si un branch devait cibler une tache d'un groupe, il faudrait renvoyer ce nom complet.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import dag, task, task_group
from airflow.providers.standard.operators.empty import EmptyOperator

default_args = {
    "owner": "formation",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="snowystore_etl_tp7_from_tp1",
    description="Fil rouge Snowystore : branching, TaskGroup et mapping dynamique",
    start_date=datetime(2026, 9, 4),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["snowystore", "tp7"],
)
def snowystore_etl():

    @task
    def extract(**context) -> int:
        print(f"Extraction des commandes du {context['ds']}")
        return 1247

    # ── MISSION 3 (preparation) ──────────────────────────────────────────────
    # La liste vient d'une TACHE : son contenu n'est donc connu qu'a l'EXECUTION.
    # C'est toute la difference avec une boucle Python, qui figerait le nombre
    # de taches au moment du parsing du fichier.
    @task
    def get_categories() -> list[str]:
        categories = ["ski", "snowboard", "textile", "chaussures", "accessoires"]
        print(f"{len(categories)} categories a traiter : {categories}")
        return categories

    # ── MISSION 1 : le branchement ───────────────────────────────────────────
    # La fonction renvoie le task_id de la branche a suivre.
    # Toutes les autres branches sont automatiquement SKIPPEES.
    @task.branch
    def choose_load_mode(**context) -> str:
        jour = context["logical_date"].weekday()      # 0 = lundi
        choix = "full_load" if jour == 0 else "incremental_load"
        print(f"Jour de la semaine : {jour} -> strategie retenue : {choix}")
        return choix

    @task
    def full_load() -> None:
        print("Rechargement COMPLET (nous sommes lundi)")

    @task
    def incremental_load() -> None:
        print("Chargement INCREMENTAL (jour ordinaire)")

    # ── MISSION 4 : la jointure ──────────────────────────────────────────────
    # Sans trigger_rule, cette tache serait skippee : la branche non retenue est
    # "skipped", et la regle par defaut (all_success) l'exige "success".
    join = EmptyOperator(
        task_id="join",
        trigger_rule="none_failed_min_one_success",
    )

    # ── MISSION 2 + 3 : TaskGroup + mapping dynamique ────────────────────────
    @task
    def transform_category(categorie: str, env: str) -> dict:
        """Une INSTANCE de cette tache sera creee par categorie."""
        total = round(len(categorie) * 1234.5, 2)     # calcul fictif
        print(f"[{env}] Categorie {categorie} -> total {total}")
        return {"categorie": categorie, "total": total}

    @task_group(group_id="transformations")
    def transformations(categories: list[str]):
        # .partial() fige les arguments CONSTANTS
        # .expand() itere sur la liste -> N taches a l'execution
        return transform_category.partial(env="formation").expand(categorie=categories)

    # ── Reduction : la tache aval recoit la LISTE de tous les resultats ───────
    @task
    def summarize(resultats: list[dict]) -> None:
        total_global = sum(r["total"] for r in resultats)
        print(f"{len(resultats)} categories traitees")
        print(f"Chiffre d'affaires global : {total_global}")

    # ── L'assemblage ─────────────────────────────────────────────────────────
    nb_lignes = extract()
    categories = get_categories()

    branche = choose_load_mode()
    branche >> [full_load(), incremental_load()] >> join

    resultats = transformations(categories)
    join >> resultats                      # les transformations attendent la jointure

    summarize(resultats)

    nb_lignes >> branche                   # l'extraction precede le branchement


snowystore_etl()

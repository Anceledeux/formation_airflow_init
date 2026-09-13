# Jour 1 — Mettre en place le projet de formation
---

## Étape 1 — Arrêter le projet de test

C'est **indispensable** : deux projets Airflow ne peuvent pas tourner en même temps, ils se
disputeraient le port 8080.

> ⚠️ Les commandes `astro` s'appliquent au **dossier dans lequel vous êtes**. Il faut donc être
> dans `test-airflow` pour l'arrêter.

```bash
cd chemin/vers/test-airflow
astro dev stop
```

Vérifiez qu'il ne reste rien :

```bash
podman ps          # la liste doit être vide ou ne plus contenir de conteneur Airflow
```

Vous pouvez ensuite **supprimer le dossier `test-airflow`** : il ne servira plus. L'image
Airflow, elle, reste en cache — c'est tout l'intérêt de l'avoir téléchargée en avance.

---

## Étape 2 — Où décompresser le projet

Choisissez un dossier de travail **simple**, à la racine de votre espace personnel :

| Système | Exemple |
|---------|---------|
| Windows | `C:\Users\vous\formation-airflow` |
| macOS / Linux | `~/formation-airflow` |

> ⚠️ **Évitez les dossiers synchronisés OneDrive, SharePoint ou Dropbox.** Ils provoquent des
> verrous de fichiers et de fortes lenteurs avec les conteneurs. C'est une cause classique de
> problèmes inexplicables.

Décompressez-y `snowystore-airflow.zip`.

### Vérifiez que vous êtes dans le bon dossier

La décompression crée parfois un **dossier imbriqué** (`snowystore-airflow/snowystore-airflow`).

**Le bon dossier est celui qui contient le fichier `Dockerfile`.** Placez-vous dedans et
vérifiez :

```bash
cd formation-airflow/snowystore-airflow
ls          # macOS, Linux, Git Bash
```
```powershell
dir         # Windows PowerShell
```

Vous devez voir : **`Dockerfile`**, `requirements.txt`, `airflow_settings.yaml`,
`docker-compose.override.yml`, et les dossiers `dags/`, `include/`, `solutions/`, `scripts/`,
`tests/`.

Vérifiez aussi la présence du dossier caché **`.astro`** :

```bash
ls -a | grep astro
```
```powershell
dir -Force | Select-String astro
```

> **Si `.astro` est absent**, l'Astro CLI refusera de démarrer avec le message
> *« this is not an Astro project directory »*. Créez-le :
>
> ```bash
> mkdir -p .astro
> printf 'project:\n  name: snowystore-airflow\n' > .astro/config.yaml
> ```

---

## Étape 3 — Recaler les dates sur la session

Les exercices lisent un fichier de commandes par jour (`orders_AAAA-MM-JJ.csv`). Ce script
ajuste les dates du projet sur **la date d'aujourd'hui** et génère les données correspondantes.

**Remplacez la date par celle du premier jour de formation :**

```bash
./scripts/set_start_date.sh 2026-09-14
```
```powershell
.\scripts\set_start_date.ps1 2026-09-14
```

Pour voir ce qui serait modifié sans rien changer, ajoutez `--dry-run` (Bash) ou `-DryRun`
(PowerShell).

### Vérifiez

```bash
ls include/data | head -3
ls include/data | tail -3
```
```powershell
dir include\data | Select-Object -First 3
dir include\data | Select-Object -Last 3
```

Vous devez obtenir une quinzaine de fichiers `orders_….csv`, dont les dates **encadrent les
trois jours de formation**.

> **Sous Git Bash**, si le script refuse de s'exécuter :
> `chmod +x scripts/*.sh`

---

## Étape 4 — Démarrer

```bash
astro dev start
```

**Pas besoin de `restart`** : c'est un projet neuf, les conteneurs sont créés de zéro.

Vous devez voir défiler :

```
Added Pool: dwh_pool
Added Variable: snowystore_env
Added Variable: snowystore_config
Added Connection: fs_default
Added Connection: snowystore_dwh
➤ Airflow UI: http://localhost:8080
```

Ces lignes confirment que le fichier `airflow_settings.yaml` a bien été pris en compte.

> Sur Windows, un avertissement `could not start proxy: proxy daemon is not supported on
> Windows` peut apparaître : **il est sans effet**, ignorez-le.

Ouvrez **http://localhost:8080** — identifiants `admin` / `admin`.

---

## Étape 5 — Les quatre vérifications

| # | Où | Ce que vous devez voir |
|---|-----|------------------------|
| 1 | http://localhost:8080 | l'interface s'ouvre, connexion OK |
| 2 | **Admin → Connections** | **deux** entrées : `snowystore_dwh` et `fs_default` |
| 3 | **Admin → Pools** | `dwh_pool` avec **2** slots |
| 4 | Terminal | des fichiers dans `include/data` |

### Deux choses normales, qui ne sont pas des erreurs

- **« Aucun DAG trouvé »** : le dossier `dags/` est volontairement vide. C'est vous qui allez
  écrire les DAGs pendant les exercices.
- **Une erreur d'importation sur `snowystore_broken.py`** : ce fichier est **cassé exprès**,
  c'est le support d'un exercice de débogage cet après-midi. Ne le corrigez pas, et n'ouvrez pas
  son corrigé dans `solutions/`.

---

## Ce que contient le projet

```
snowystore-airflow/
├── dags/              vos DAGs — c'est ici que vous travaillez
├── include/           données du fil rouge et logique métier
├── solutions/         les corrigés de chaque exercice
├── tests/             tests d'intégrité et tests unitaires
├── scripts/           outillage (.sh et .ps1)
├── Dockerfile         fige la version d'Airflow
├── requirements.txt   providers additionnels
└── airflow_settings.yaml   connexions, variables et pool
```

**Trois documents à connaître :**

| Fichier | Quand le lire |
|---------|---------------|
| `README.md` | le point d'entrée, à parcourir maintenant |
| `LE_CONTENEUR.md` | **à lire aujourd'hui** : quand et comment entrer dans le conteneur |
| `INSTALLATION.md` | en cas de problème technique |

---

## Les commandes du quotidien

| Commande | Effet |
|----------|-------|
| `astro dev start` | démarre l'environnement |
| `astro dev stop` | arrête, sans rien perdre |
| `astro dev restart` | **après modification du `Dockerfile` ou de `requirements.txt`** |
| `astro dev kill` | remet tout à zéro (les fichiers sont conservés) |
| `astro dev bash` | ouvre un terminal **dans** le conteneur |
| `astro dev logs -f` | suit les logs |

> **Le piège le plus courant :** ajouter un provider dans `requirements.txt` puis faire
> `stop` + `start`. L'image n'est pas reconstruite, et le provider reste introuvable.
> Il faut **`astro dev restart`**.

Sous **Git Bash** sur Windows, préfixez les commandes interactives par `winpty` :
`winpty astro dev bash`.

---

## Si les corrigés vous manquent

Le fil rouge est **cumulatif** : chaque exercice repart du précédent. Si vous décrochez, vous
pouvez repartir d'une base saine :

```bash
./scripts/reset_tp.sh --list       # liste les points de reprise
./scripts/reset_tp.sh tp05         # installe le corrigé du TP5
```
```powershell
.\scripts\reset_tp.ps1 -List
.\scripts\reset_tp.ps1 tp05
```

Votre travail précédent est sauvegardé dans `dags/_backup/`.

**La règle :** cherchez d'abord, appelez le formateur ensuite, ouvrez le corrigé en dernier
recours. Et surtout, **n'ouvrez pas** `solutions/tp02bis_debug_corrige.py` avant le débrief de
l'exercice de débogage.

---

## En cas de problème

| Message | Cause | Solution |
|---------|-------|----------|
| `this is not an Astro project directory` | mauvais dossier, ou `.astro` absent | voir étape 2 |
| `port 8080 already in use` | le projet de test tourne encore | `cd test-airflow` puis `astro dev stop` |
| `cannot connect to Podman` | machine virtuelle arrêtée | `podman machine start` |
| `FileNotFoundError: orders_….csv` | données non générées | refaire l'étape 3 |
| Le DAG n'apparaît pas | erreur d'import | `astro dev bash` puis `airflow dags list-import-errors` |
| Tâches bloquées « en file » | **le DAG est en pause** | activer l'interrupteur à gauche de son nom |
| Provider introuvable | image non reconstruite | `astro dev restart` |

# Support de cours — Introduction à dbt, Snowflake et Jinja

---

## Table des matières

1. [Introduction à dbt](#1-introduction-à-dbt)
2. [Pourquoi apprendre dbt ?](#2-pourquoi-apprendre-dbt-)
3. [Organisation et objectifs du cours](#3-organisation-et-objectifs-du-cours)
4. [Quand utiliser dbt ?](#4-quand-utiliser-dbt-)
5. [dbt Core vs dbt Cloud](#5-dbt-core-vs-dbt-cloud)
6. [Chapitre 1 — Mise en place de l'environnement](#6-chapitre-1--mise-en-place-de-lenvironnement)
   - 6.1 [Création du compte Snowflake](#61-création-du-compte-snowflake)
   - 6.2 [Configuration de Snowflake](#62-configuration-de-snowflake)
   - 6.3 [Création du compte dbt Cloud](#63-création-du-compte-dbt-cloud)
   - 6.4 [Connexion dbt Cloud ↔ Snowflake](#64-connexion-dbt-cloud--snowflake)
   - 6.5 [Initialisation du projet dbt](#65-initialisation-du-projet-dbt)
7. [Le jeu de données Airbnb Amsterdam](#7-le-jeu-de-données-airbnb-amsterdam)
   - 7.1 [Source et description](#71-source-et-description)
   - 7.2 [Structure des tables](#72-structure-des-tables)
   - 7.3 [Chargement des données dans Snowflake](#73-chargement-des-données-dans-snowflake)
8. [Introduction au langage Jinja](#8-introduction-au-langage-jinja)
   - 8.1 [Qu'est-ce que Jinja ?](#81-quest-ce-que-jinja-)
   - 8.2 [Les délimiteurs Jinja](#82-les-délimiteurs-jinja)
   - 8.3 [Boucles et conditions](#83-boucles-et-conditions)
   - 8.4 [Les macros Jinja](#84-les-macros-jinja)
   - 8.5 [Jinja dans dbt — le lien essentiel](#85-jinja-dans-dbt--le-lien-essentiel)
9. [Annexes](#9-annexes)

---

## 1. Introduction à dbt

### C'est quoi dbt ?

**dbt** (data build tool) est un outil SQL puissant qui s'inscrit dans la couche **T** (Transform) du paradigme **ELT** (Extract, Load, Transform). Il permet aux Data Analysts et Data Engineers d'écrire les **transformations** de leurs données en SQL tout en bénéficiant des bonnes pratiques du génie logiciel.

Concrètement, dbt permet :

- D'écrire les **transformations** de données en SQL pur
- De **ne pas répéter** son code SQL grâce à la **modularisation** et à la **paramétrisation** (via Jinja)
- De **tester** son code SQL en isolation, mais aussi de voir comment il s'intègre à la plateforme analytique globale
- Aux entreprises de s'assurer de la **fiabilité des données**
- D'appliquer les bonnes règles de **data gouvernance**

### Le DAG dbt (Directed Acyclic Graph)

L'un des concepts centraux de dbt est le **lineage graph** (ou DAG). Il s'agit d'une représentation visuelle des dépendances entre vos modèles SQL. Chaque nœud représente un modèle, et chaque flèche représente une dépendance. Ce graphe permet de comprendre en un coup d'œil comment les données circulent depuis les sources brutes jusqu'aux tables finales utilisées par les analystes.

> **Exemple** : dans un projet typique, on pourrait avoir des modèles `staging` (nettoyage) qui alimentent des modèles `intermediate` (logique métier), qui alimentent à leur tour des modèles `marts` (tables finales pour l'analyse).

---

## 2. Pourquoi apprendre dbt ?

dbt est devenu un outil **indispensable** dans le travail des **professionnels** de la Data. Ceci est d'autant plus vrai avec l'essor du rôle d'**Analytics Engineer**, ce rôle hybride entre analyste et ingénieur des données.

On s'attend à ce que la demande des entreprises pour cette technologie **croisse** dans les années à venir. Maîtriser dbt, c'est se positionner sur l'un des outils les plus demandés dans l'écosystème data moderne.

---

## 3. Organisation et objectifs du cours

Ce cours est un programme complet qui couvre les sujets suivants :

1. Introduction à dbt Cloud et Core et mise en place de l'environnement de travail
2. Introduction au langage Jinja
3. Développer des modèles dbt
4. Gérer les matérialisations sous dbt
5. Créer des seeds et des snapshots dbt
6. Écrire des tests unitaires
7. Écrire des macros
8. Documenter ses modèles
9. Développer des analyses avec SQL
10. Travailler avec les variables sous dbt

> **Objectif global** : Un cours complet qui vous permettra de commencer à développer sous dbt en toute confiance.

---

## 4. Quand utiliser dbt ?

L'intérêt d'utiliser dbt **croît avec la complexité** de la plateforme analytique de l'entreprise.

- Écrire un simple `SELECT *` ne nécessite probablement **pas** la mise en place de dbt
- Par contre, si on a un **grand nombre de SQL complexes interdépendants**, dbt vous permettra d'**économiser beaucoup de temps**

**Règle générale** : dès que vous commencez à avoir des dépendances entre vos requêtes SQL (une requête qui utilise le résultat d'une autre), dbt commence à apporter une vraie valeur ajoutée.

---

## 5. dbt Core vs dbt Cloud

| Critère | dbt Core | dbt Cloud |
|---------|----------|-----------|
| **Accès payant/gratuit** | Open source, gratuit | Payant, dans le cloud |
| **Requiert le maintien d'une infrastructure** | Oui, pour exécuter le code | Non, tout est géré par dbt Cloud |
| **Accès aux fonctionnalités essentielles de dbt** | Oui | Oui + fonctionnalités payantes supplémentaires |

> **Pour ce cours**, nous utiliserons **dbt Cloud** qui offre un accès gratuit sans limite de temps pour un développeur individuel (plan « Developer »).

---

## 6. Chapitre 1 — Mise en place de l'environnement

### 6.1 Création du compte Snowflake

La première étape consiste à créer un compte gratuit sur Snowflake. Snowflake propose un essai gratuit de 30 jours avec $400 de crédits.

Rendez-vous sur le site de Snowflake et suivez le processus d'inscription. Vous obtiendrez une URL de la forme :

```
https://<votre-identifiant>.snowflakecomputing.com
```

> **Important** : notez bien votre identifiant de compte (la partie avant `.snowflakecomputing.com`). Vous en aurez besoin pour configurer la connexion avec dbt Cloud.

---

### 6.2 Configuration de Snowflake

Une fois votre compte Snowflake créé, il faut préparer l'environnement pour dbt. Copiez-collez le script SQL ci-dessous dans l'éditeur Snowflake (Worksheets) et exécutez-le.

```sql
-- ============================================================
-- ÉTAPE 1 : Utiliser le rôle administrateur
-- ============================================================
-- Le rôle ACCOUNTADMIN a tous les droits. On l'utilise pour
-- créer les objets nécessaires.
USE ROLE ACCOUNTADMIN;

-- ============================================================
-- ÉTAPE 2 : Créer le rôle 'transform'
-- ============================================================
-- Ce rôle sera dédié à dbt. Il aura uniquement les permissions
-- nécessaires pour transformer les données (principe du moindre
-- privilège).
CREATE ROLE IF NOT EXISTS transform;
GRANT ROLE TRANSFORM TO ROLE ACCOUNTADMIN;

-- ============================================================
-- ÉTAPE 3 : Créer la warehouse (entrepôt de calcul)
-- ============================================================
-- La warehouse est la ressource de calcul qui exécute les
-- requêtes SQL. COMPUTE_WH est la warehouse par défaut.
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;

-- ============================================================
-- ÉTAPE 4 : Créer l'utilisateur dbt
-- ============================================================
-- Cet utilisateur sera utilisé par dbt Cloud pour se connecter
-- à Snowflake. On lui assigne le rôle 'transform'.
CREATE USER IF NOT EXISTS dbt
  PASSWORD='MotDePasseDBT123@'
  LOGIN_NAME='dbt'
  TYPE=LEGACY_SERVICE
  MUST_CHANGE_PASSWORD=FALSE
  DEFAULT_WAREHOUSE='COMPUTE_WH'
  DEFAULT_ROLE='transform'
  DEFAULT_NAMESPACE='AIRBNB.RAW'
  COMMENT='Utilisateur DBT pour la transformation des données';

GRANT ROLE transform TO USER dbt;

-- ============================================================
-- ÉTAPE 5 : Créer la base de données et le schéma
-- ============================================================
-- La base AIRBNB contiendra toutes nos données.
-- Le schéma RAW contiendra les données brutes (sources).
CREATE DATABASE IF NOT EXISTS AIRBNB;
CREATE SCHEMA IF NOT EXISTS AIRBNB.RAW;

-- ============================================================
-- ÉTAPE 6 : Configurer les permissions du rôle 'transform'
-- ============================================================
-- On donne au rôle 'transform' l'accès complet à la warehouse,
-- la base de données, les schémas et les tables (existantes
-- et futures).
GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE transform;
GRANT ALL ON DATABASE AIRBNB TO ROLE transform;
GRANT ALL ON ALL SCHEMAS IN DATABASE AIRBNB TO ROLE transform;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE AIRBNB TO ROLE transform;
GRANT ALL ON ALL TABLES IN SCHEMA AIRBNB.RAW TO ROLE transform;
GRANT ALL ON FUTURE TABLES IN SCHEMA AIRBNB.RAW TO ROLE transform;
```

**Explication des concepts clés :**

- **ROLE** : un rôle Snowflake est un ensemble de permissions. Le modèle de sécurité de Snowflake est basé sur le RBAC (Role-Based Access Control).
- **WAREHOUSE** : dans Snowflake, une warehouse est une ressource de calcul (cluster de serveurs virtuels). Elle ne stocke pas de données — elle les traite.
- **GRANT** : commande SQL pour accorder des permissions à un rôle ou un utilisateur.
- **FUTURE** : le mot-clé `FUTURE` dans les `GRANT` permet d'accorder automatiquement les permissions aux objets qui seront créés plus tard.

> **Sécurité** : n'hésitez pas à changer le mot de passe `MotDePasseDBT123@` par un mot de passe plus sécurisé avant d'exécuter le script.

---

### 6.3 Création du compte dbt Cloud

Rendez-vous sur **https://www.getdbt.com/** et cliquez sur **"Create a free account"**.

**Étape 1 — Inscription**

Remplissez le formulaire d'inscription avec votre email, prénom, nom, entreprise et mot de passe. L'offre « Free forever for one developer » est gratuite et sans engagement.

**Étape 2 — Nommer le projet**

Une fois connecté, dbt vous demande de configurer un nouveau projet. Donnez-lui le nom **`Analytics`** et cliquez sur **Continue**.

**Étape 3 — Configurer l'environnement de développement**

Dans la section « Configure your development environment », cliquez sur le menu déroulant **Connection** puis sur **"+ Add new connection"** pour ajouter une connexion à votre warehouse.

**Étape 4 — Ajouter la connexion Snowflake**

Sur la page « Add new connection » :

1. Sélectionnez **Snowflake** parmi les options proposées (PostgreSQL, BigQuery, Redshift, Databricks, etc.)
2. Donnez un nom à votre connexion (par exemple « Snowflake »)
3. Remplissez les champs :
   - **Account** : votre identifiant Snowflake (ex : `jchlzda-ucb30964`)
   - **Database** : `AIRBNB`
   - **Warehouse** : `COMPUTE_WH`

**Étape 5 — Configurer les credentials de développement**

Sur la page « Development credentials » :

| Champ | Valeur |
|-------|--------|
| Auth method | Username and password |
| Username | `dbt` |
| Password | *(le mot de passe défini dans le script Snowflake)* |
| Schema | `RAW` |
| Target name | `default` |
| Threads | `6` |

Une barre de progression en bas confirme les étapes :
1. **SETUP** — préparation du test
2. **TESTING CONNECTION** — validation des paramètres
3. **COMPLETE** — « Your test completed successfully, you're good to go! »

Cliquez sur **Save**.

**Étape 6 — Configurer le repository**

Dans la section « Setup a repository », choisissez l'option **Managed** (le plus simple pour débuter). Nommez votre repository **`projet_dbt`** et cliquez sur **Create**.

> **Note** : l'option « Managed » signifie que dbt Cloud héberge le code pour vous. Les autres options (Git Clone, GitHub, GitLab) permettent d'utiliser un repository Git externe.

---

### 6.4 Connexion dbt Cloud ↔ Snowflake

Une fois les étapes précédentes terminées, vous arrivez sur la page **"Your project is ready!"** avec plusieurs options pour démarrer. Cliquez sur **Develop > Cloud IDE** pour ouvrir l'environnement de développement intégré.

---

### 6.5 Initialisation du projet dbt

Au premier accès à l'IDE, vous verrez un explorateur de fichiers quasi vide avec juste le nom de votre projet. 

Cliquez sur le bouton vert **"Initialize dbt project"** dans le panneau Version Control. dbt va automatiquement créer la structure de base du projet.

Après initialisation, la structure suivante apparaît dans l'explorateur de fichiers :

```
projet_dbt/
├── analyses/          ← Requêtes SQL ad hoc (exploratoires)
├── macros/            ← Fonctions Jinja réutilisables
├── models/            ← Les modèles SQL (cœur du projet)
│   ├── my_first_dbt_model.sql
│   ├── my_second_dbt_model.sql
│   └── schema.yml
├── seeds/             ← Fichiers CSV chargés comme tables
├── snapshots/         ← Captures d'état pour le SCD (Slowly Changing Dimensions)
├── tests/             ← Tests personnalisés
├── .gitignore
├── dbt_project.yml    ← Fichier de configuration principal
└── README.md
```

**Description des dossiers clés :**

- **`models/`** : contient les fichiers `.sql` qui définissent vos transformations. C'est ici que vous passerez la majorité de votre temps.
- **`macros/`** : contient les macros Jinja réutilisables (fonctions paramétrées).
- **`seeds/`** : contient des fichiers CSV que dbt peut charger directement dans votre warehouse (utile pour des données de référence).
- **`snapshots/`** : contient les définitions de snapshots pour capturer l'historique des changements.
- **`tests/`** : contient des tests SQL personnalisés.
- **`dbt_project.yml`** : le fichier de configuration central qui définit le nom du projet, les chemins, les matérialisations par défaut, etc.

> **Dernière étape** : cliquez sur **"Commit and sync"** pour sauvegarder l'initialisation dans le repository.

---

## 7. Le jeu de données Airbnb Amsterdam

### 7.1 Source et description

Le jeu de données a été téléchargé depuis le site **https://insideairbnb.com/get-the-data/** qui regroupe les données Airbnb pour plusieurs villes à travers le monde. Pour ce cours, nous avons choisi la ville d'**Amsterdam** correspondant à un extrait du **11 Mars 2024**.

Le fichier original `listings.csv.gz` a été divisé et simplifié pour créer un jeu de données pédagogique composé de 3 fichiers, hébergés sur GitHub : **https://github.com/QuantikDataStudio/dbt**

**Traitement appliqué :**

1. **Division** du fichier `listings.csv.gz` en 2 fichiers :
   - `listings.csv` : colonnes réduites, uniquement les données liées au listing (on a retiré les infos sur l'hôte et les revues)
   - `hosts.csv` : extrait du fichier original, contient uniquement les informations de l'hôte
2. `reviews.csv` : téléchargé du jeu de données résumé, contient seulement 2 colonnes (`listing_id` et `date`)

Un fichier complémentaire `tourists_per_year.csv` contient le nombre annuel de touristes à Amsterdam (2012-2023). Ce fichier pourra être utilisé comme **seed** dbt dans les chapitres suivants.

---

### 7.2 Structure des tables

#### Table `RAW.HOSTS` — 7 815 lignes

| Colonne | Type | Description |
|---------|------|-------------|
| `host_id` | STRING | Identifiant unique de l'hôte |
| `host_name` | STRING | Prénom de l'hôte |
| `host_since` | DATE | Date d'inscription sur Airbnb |
| `host_location` | STRING | Lieu de résidence déclaré |
| `host_response_time` | STRING | Temps de réponse moyen (ex : « within an hour ») |
| `host_response_rate` | STRING | Taux de réponse (ex : « 100% ») |
| `host_is_superhost` | STRING | Statut superhost : `t` (true) ou `f` (false) |
| `host_neighbourhood` | STRING | Quartier de l'hôte |
| `host_identity_verified` | STRING | Identité vérifiée : `t` ou `f` |

#### Table `RAW.LISTINGS` — 8 943 lignes

| Colonne | Type | Description |
|---------|------|-------------|
| `id` | STRING | Identifiant unique du listing |
| `listing_url` | STRING | URL Airbnb du listing |
| `name` | STRING | Titre de l'annonce |
| `description` | STRING | Description détaillée |
| `neighbourhood_overview` | STRING | Description du quartier |
| `host_id` | STRING | Clé étrangère vers `HOSTS` |
| `latitude` | STRING | Latitude GPS |
| `longitude` | STRING | Longitude GPS |
| `property_type` | STRING | Type de bien (ex : « Entire rental unit ») |
| `room_type` | STRING | Type de chambre (ex : « Private room ») |
| `accommodates` | INTEGER | Nombre de personnes accueillies |
| `bathrooms` | FLOAT | Nombre de salles de bain |
| `bedrooms` | FLOAT | Nombre de chambres |
| `beds` | FLOAT | Nombre de lits |
| `amenities` | STRING | Liste des équipements (format JSON) |
| `price` | STRING | Prix par nuit (ex : « $950.00 ») |
| `minimum_nights` | INTEGER | Nombre minimum de nuits |
| `maximum_nights` | INTEGER | Nombre maximum de nuits |

#### Table `RAW.REVIEWS` — 402 663 lignes

| Colonne | Type | Description |
|---------|------|-------------|
| `listing_id` | STRING | Clé étrangère vers `LISTINGS` |
| `date` | DATE | Date du commentaire laissé |

**Exemple de lecture** : les lignes `262394,2012-04-11` et `262394,2012-04-25` indiquent que le listing 262394 a reçu 2 commentaires : un le 11 avril 2012 et l'autre le 25 avril 2012.

#### Table complémentaire `tourists_per_year` (potentiel seed dbt)

| Colonne | Type | Description |
|---------|------|-------------|
| `year` | INTEGER | Année |
| `tourists` | INTEGER | Nombre de touristes à Amsterdam |

Données couvrant la période 2012-2023 (12 lignes).

---

### 7.3 Chargement des données dans Snowflake

Pour charger les données depuis GitHub directement dans Snowflake, exécutez le script SQL suivant dans votre éditeur Snowflake.

```sql
-- ============================================================
-- ÉTAPE 1 : Se positionner sur les bons objets
-- ============================================================
USE WAREHOUSE COMPUTE_WH;
USE DATABASE AIRBNB;
USE SCHEMA RAW;

-- ============================================================
-- ÉTAPE 2 : Créer une intégration API vers GitHub
-- ============================================================
-- L'API INTEGRATION permet à Snowflake de se connecter à un
-- service externe (ici GitHub) pour récupérer des fichiers.
-- On autorise uniquement le préfixe GitHub de notre repository.
CREATE OR REPLACE API INTEGRATION integration_jeu_de_donnees_github
  api_provider = git_https_api
  api_allowed_prefixes = ('https://github.com/QuantikDataStudio')
  enabled = true;

-- ============================================================
-- ÉTAPE 3 : Créer un repository Git dans Snowflake
-- ============================================================
-- Ce repository est un "stage" spécial qui pointe vers un
-- dépôt GitHub. Snowflake pourra ensuite lire les fichiers
-- directement depuis ce stage.
CREATE OR REPLACE GIT REPOSITORY jeu_de_donnees_airbnb
  api_integration = integration_jeu_de_donnees_github
  origin = 'https://github.com/QuantikDataStudio/dbt.git';

-- ============================================================
-- ÉTAPE 4 : Définir le format des fichiers CSV
-- ============================================================
-- On indique à Snowflake comment interpréter nos CSV :
-- - skip_header = 1 : ignorer la première ligne (en-têtes)
-- - field_optionally_enclosed_by : les champs peuvent être
--   entourés de guillemets doubles
CREATE OR REPLACE FILE FORMAT format_jeu_de_donnees
  type = csv
  skip_header = 1
  field_optionally_enclosed_by = '"';

-- ============================================================
-- ÉTAPE 5 : Créer et peupler la table HOSTS
-- ============================================================
CREATE TABLE AIRBNB.RAW.HOSTS
(
  host_id                 STRING,
  host_name               STRING,
  host_since              DATE,
  host_location           STRING,
  host_response_time      STRING,
  host_response_rate      STRING,
  host_is_superhost       STRING,
  host_neighbourhood      STRING,
  host_identity_verified  STRING
);

-- $1, $2, ... $N réfèrent aux colonnes positionnelles du CSV.
-- Le @ désigne un "stage" Snowflake (ici notre repository Git).
INSERT INTO AIRBNB.RAW.HOSTS
(SELECT $1 AS host_id,
        $2 AS host_name,
        $3 AS host_since,
        $4 AS host_location,
        $5 AS host_response_time,
        $6 AS host_response_rate,
        $7 AS host_is_superhost,
        $8 AS host_neighbourhood,
        $9 AS host_identity_verified
 FROM @jeu_de_donnees_airbnb/branches/main/dataset/hosts.csv
 (FILE_FORMAT => 'format_jeu_de_donnees'));

-- ============================================================
-- ÉTAPE 6 : Créer et peupler la table LISTINGS
-- ============================================================
CREATE TABLE AIRBNB.RAW.LISTINGS
(
  id                      STRING,
  listing_url             STRING,
  name                    STRING,
  description             STRING,
  neighbourhood_overview  STRING,
  host_id                 STRING,
  latitude                STRING,
  longitude               STRING,
  property_type           STRING,
  room_type               STRING,
  accommodates            INTEGER,
  bathrooms               FLOAT,
  bedrooms                FLOAT,
  beds                    FLOAT,
  amenities               STRING,
  price                   STRING,
  minimum_nights          INTEGER,
  maximum_nights          INTEGER
);

INSERT INTO AIRBNB.RAW.LISTINGS
(SELECT $1  AS id,
        $2  AS listing_url,
        $3  AS name,
        $4  AS description,
        $5  AS neighbourhood_overview,
        $6  AS host_id,
        $7  AS latitude,
        $8  AS longitude,
        $9  AS property_type,
        $10 AS room_type,
        $11 AS accommodates,
        $12 AS bathrooms,
        $13 AS bedrooms,
        $14 AS beds,
        $15 AS amenities,
        $16 AS price,
        $17 AS minimum_nights,
        $18 AS maximum_nights
 FROM @jeu_de_donnees_airbnb/branches/main/dataset/listings.csv
 (FILE_FORMAT => 'format_jeu_de_donnees'));

-- ============================================================
-- ÉTAPE 7 : Créer et peupler la table REVIEWS
-- ============================================================
CREATE TABLE AIRBNB.RAW.REVIEWS
(
  listing_id  STRING,
  date        DATE
);

INSERT INTO AIRBNB.RAW.REVIEWS
(SELECT $1 AS listing_id,
        $2 AS date
 FROM @jeu_de_donnees_airbnb/branches/main/dataset/reviews.csv
 (FILE_FORMAT => 'format_jeu_de_donnees'));
```

**Concepts Snowflake importants dans ce script :**

- **API INTEGRATION** : objet Snowflake qui gère l'authentification vers un service externe. Ici, on autorise les connexions vers `https://github.com/QuantikDataStudio`.
- **GIT REPOSITORY** : fonctionnalité récente de Snowflake qui permet de monter un dépôt Git comme un « stage » (espace de stockage). On peut ensuite lire les fichiers du dépôt avec la notation `@nom_du_repo/branches/main/chemin/fichier.csv`.
- **FILE FORMAT** : définition du format de lecture des fichiers. Les paramètres `skip_header` et `field_optionally_enclosed_by` sont essentiels pour que Snowflake interprète correctement le CSV.
- **Colonnes positionnelles `$1`, `$2`, etc.** : quand on lit un fichier depuis un stage, Snowflake attribue un numéro à chaque colonne dans l'ordre d'apparition. On les renomme avec `AS`.
- **Le préfixe `@`** : dans Snowflake, `@` désigne toujours un stage (zone de stockage temporaire ou externe).

> **Résultat attendu** : après exécution, la table `REVIEWS` devrait contenir **402 663 lignes** (vérifiable dans le panneau de résultats Snowflake).

---

## 8. Introduction au langage Jinja

### 8.1 Qu'est-ce que Jinja ?

**Jinja** est un moteur de templates écrit en Python. Il permet de générer du texte dynamique à partir de modèles (templates) et de données.

Dans le contexte de dbt, Jinja est le « super-pouvoir » qui transforme le SQL statique en SQL dynamique et paramétrable. Grâce à Jinja, on peut écrire des boucles, des conditions, des macros et des variables directement dans nos fichiers SQL.

> **Analogie** : si SQL est le langage qui parle à la base de données, Jinja est le langage qui écrit le SQL pour vous.

Pour ce cours d'introduction, nous utilisons Jinja en Python pur (via la bibliothèque `jinja2`) pour comprendre les concepts de base. Dans les chapitres suivants, nous verrons comment ces mêmes concepts s'appliquent dans dbt.

```python
# Installation (si nécessaire) : pip install jinja2
from jinja2 import Environment, BaseLoader
```

---

### 8.2 Les délimiteurs Jinja

Jinja utilise trois types de délimiteurs, chacun avec un rôle distinct :

| Délimiteur | Rôle | Exemple |
|------------|------|---------|
| `{{ ... }}` | **Expression** : affiche la valeur d'une variable ou le résultat d'une expression | `{{ user.nom }}` affiche le nom |
| `{% ... %}` | **Instruction** : exécute une logique (boucle, condition, macro) sans rien afficher | `{% for u in users %}` |
| `{# ... #}` | **Commentaire** : ignoré lors du rendu | `{# Ceci est un commentaire #}` |

> **Règle mnémotechnique** : les accolades doubles `{{ }}` sont pour **montrer**, les accolades-pourcentage `{% %}` sont pour **faire**.

---

### 8.3 Boucles et conditions

Jinja supporte les boucles `for` et les conditions `if / elif / else`, avec une syntaxe très proche de Python.

**Exemple complet — Template Jinja :**

```jinja
{% for user in users %}
  {% if user.pays == "Espagne" %}
    {{ user.nom }} vive en españa
  {% elif user.pays == "USA" %}
    {{ user.nom }} is in the USA
  {% elif user.pays == "France" %}
    {{ user.nom }} vit en France
  {% else %}
    {{ user.nom }} vit ailleurs
  {% endif %}
{% endfor %}
```

**Données injectées dans le template :**

```python
data = [
    {"nom": "Adam",   "occupation": "enseignant",    "ville": "Barcelone",    "pays": "Espagne"},
    {"nom": "Paul",   "occupation": "etudiant",      "ville": "Paris",        "pays": "France"},
    {"nom": "Thomas", "occupation": "data analyste",  "ville": "New York City","pays": "USA"},
]
```

**Rendu du template :**

```python
rtemplate = Environment(loader=BaseLoader).from_string(jinja_template)
print(rtemplate.render(users=data))
```

**Résultat obtenu :**

```
Adam vive en españa
Paul vit en France
Thomas is in the USA
```

**Décryptage ligne par ligne :**

1. `{% for user in users %}` — On itère sur chaque élément de la liste `users`. À chaque itération, la variable `user` contient un dictionnaire.
2. `{% if user.pays == "Espagne" %}` — On teste la valeur de la clé `pays` du dictionnaire courant.
3. `{{ user.nom }}` — On affiche la valeur de la clé `nom` (les doubles accolades insèrent la valeur dans le texte de sortie).
4. `{% elif ... %}` — Condition alternative (comme en Python).
5. `{% else %}` — Cas par défaut si aucune condition précédente n'est remplie.
6. `{% endif %}` — Ferme le bloc conditionnel (obligatoire en Jinja, contrairement à Python qui utilise l'indentation).
7. `{% endfor %}` — Ferme la boucle `for`.

> **Différence clé avec Python** : en Jinja, chaque bloc doit être explicitement fermé avec `{% endif %}`, `{% endfor %}`, etc. L'indentation est cosmétique, pas syntaxique.

---

### 8.4 Les macros Jinja

Une **macro** Jinja est l'équivalent d'une **fonction** en Python. Elle permet d'encapsuler un bout de template réutilisable et de le paramétrer.

**Exemple — Macro SQL pour générer des `CASE WHEN` dynamiques :**

```jinja
{% macro case_statement(liste_colonnes) -%}
  {% for colonne in liste_colonnes %}
  , CASE
      WHEN {{ colonne }} > 0 THEN 'positif'
      WHEN {{ colonne }} < 0 THEN 'negatif'
      ELSE 'nulle'
    END AS signe_{{ colonne }}
  {% endfor %}
{%- endmacro %}

SELECT
  colonne_1
  {{ case_statement(liste_colonnes) }}
FROM my_table
```

**Appel du template avec des paramètres :**

```python
template_sql = Environment(loader=BaseLoader).from_string(sql_parametrise)
print(template_sql.render(liste_colonnes=['colonne_2', 'colonne_3']))
```

**SQL généré :**

```sql
SELECT
  colonne_1
  , CASE
      WHEN colonne_2 > 0 THEN 'positif'
      WHEN colonne_2 < 0 THEN 'negatif'
      ELSE 'nulle'
    END AS signe_colonne_2
  , CASE
      WHEN colonne_3 > 0 THEN 'positif'
      WHEN colonne_3 < 0 THEN 'negatif'
      ELSE 'nulle'
    END AS signe_colonne_3
FROM my_table
```

**Décryptage ligne par ligne :**

1. `{% macro case_statement(liste_colonnes) -%}` — Déclare une macro nommée `case_statement` qui prend un paramètre `liste_colonnes`. Le `-` à la fin supprime les espaces blancs après le tag (contrôle du whitespace).
2. `{% for colonne in liste_colonnes %}` — On boucle sur chaque nom de colonne passé en paramètre.
3. `{{ colonne }}` — On insère le nom de la colonne dans le SQL généré.
4. `AS signe_{{ colonne }}` — On crée un alias dynamique en concaténant « signe_ » avec le nom de la colonne.
5. `{%- endmacro %}` — Ferme la macro. Le `-` au début supprime les espaces blancs avant le tag.
6. `{{ case_statement(liste_colonnes) }}` — On appelle la macro dans le corps du SELECT, en lui passant la variable `liste_colonnes`.

> **Contrôle des espaces blancs** : les tirets `-` dans `{%- ... -%}` permettent de « manger » les espaces et sauts de ligne autour des tags Jinja. Sans eux, le SQL généré contiendrait beaucoup de lignes vides. C'est un détail cosmétique mais important pour la lisibilité du SQL compilé.

---

### 8.5 Jinja dans dbt — le lien essentiel

La puissance de Jinja prend tout son sens dans dbt. Voici comment les concepts que nous venons de voir s'appliquent :

| Concept Jinja | Usage dans dbt |
|---------------|----------------|
| `{{ ... }}` (expressions) | Appeler des fonctions dbt comme `{{ ref('mon_modele') }}` ou `{{ source('raw', 'listings') }}` |
| `{% ... %}` (instructions) | Écrire des boucles et conditions dans les modèles SQL |
| Macros | Créer des fonctions SQL réutilisables dans le dossier `macros/` |
| Variables | Paramétrer les modèles avec `{{ var('ma_variable') }}` |

**Exemple concret dans dbt** : au lieu d'écrire un modèle SQL statique avec un nom de table en dur, on écrit :

```sql
-- models/staging/stg_listings.sql
SELECT
  id,
  name,
  host_id,
  price
FROM {{ source('raw', 'listings') }}
```

La fonction `{{ source() }}` est une macro dbt qui génère automatiquement le nom complet de la table (`AIRBNB.RAW.LISTINGS`) et enregistre la dépendance dans le DAG.

> **À retenir** : dans dbt, tout fichier `.sql` dans le dossier `models/` est en réalité un template Jinja. dbt compile d'abord le Jinja, puis exécute le SQL résultant dans le warehouse.

---

## 9. Annexes

### Liens et ressources

| Ressource | URL |
|-----------|-----|
| Inside Airbnb (source des données) | https://insideairbnb.com/get-the-data/ |
| Repository GitHub du cours | https://github.com/QuantikDataStudio/dbt |
| Site officiel dbt | https://www.getdbt.com/ |
| Documentation dbt | https://docs.getdbt.com/ |
| Documentation Jinja | https://jinja.palletsprojects.com/ |

### Récapitulatif des commandes dbt mentionnées

| Commande | Description |
|----------|-------------|
| `dbt run` | Exécute tous les modèles du projet (compile le Jinja + exécute le SQL) |
| `dbt build --select marts.marketing` | Exécute uniquement les modèles dans le dossier `marts/marketing/` |

### Glossaire

| Terme | Définition |
|-------|------------|
| **dbt** | Data Build Tool — outil de transformation de données en SQL |
| **ELT** | Extract, Load, Transform — paradigme où les données sont d'abord chargées brutes puis transformées dans le warehouse |
| **DAG** | Directed Acyclic Graph — graphe orienté sans cycle, représentant les dépendances entre modèles |
| **Warehouse** | Ressource de calcul dans Snowflake (et non stockage) |
| **Stage** | Zone de stockage intermédiaire dans Snowflake (désignée par `@`) |
| **Seed** | Fichier CSV versionné dans dbt, chargé comme table dans le warehouse |
| **Snapshot** | Mécanisme dbt pour capturer l'historique des changements d'une table source |
| **Macro** | Fonction Jinja réutilisable, stockée dans le dossier `macros/` |
| **Matérialisation** | Stratégie de stockage d'un modèle dbt (table, view, incremental, ephemeral) |
| **RBAC** | Role-Based Access Control — modèle de sécurité basé sur les rôles |
| **Analytics Engineer** | Rôle hybride entre Data Analyst et Data Engineer, spécialisé dans la transformation des données |

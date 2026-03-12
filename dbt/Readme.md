<p align="center">
  <img src="https://www.getdbt.com/assets/dbt-logo.svg" width="200">
</p>

<h1 align="center">Le Guide Complet dbt — De Débutant à Expert</h1>

<p align="center">
  <em>Cours • Documentation technique • Référence professionnelle • Template réutilisable</em>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/dbt-1.9+-FF694B?style=for-the-badge&logo=dbt&logoColor=white" alt="dbt">
  <img src="https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=postgresql&logoColor=white" alt="SQL">
  <img src="https://img.shields.io/badge/Jinja-B41717?style=for-the-badge&logo=jinja&logoColor=white" alt="Jinja">
</p>

---

## Table des matières

1. [Introduction à dbt](#1-introduction-à-dbt)
2. [Architecture d'un projet dbt](#2-architecture-dun-projet-dbt)
3. [Installation et démarrage](#3-installation-et-démarrage)
4. [Concepts fondamentaux](#4-concepts-fondamentaux)
5. [Code commenté — Exemples complets](#5-code-commenté--exemples-complets)
6. [Matérialisations — Guide complet](#6-matérialisations--guide-complet)
7. [Tests et qualité des données](#7-tests-et-qualité-des-données)
8. [Jinja et Macros](#8-jinja-et-macros)
9. [Packages dbt](#9-packages-dbt)
10. [Seeds, Snapshots et Analyses](#10-seeds-snapshots-et-analyses)
11. [Documentation du projet](#11-documentation-du-projet)
12. [Commandes dbt — Référence complète](#12-commandes-dbt--référence-complète)
13. [Sélection de nœuds — Node Selection](#13-sélection-de-nœuds--node-selection)
14. [CI/CD pour dbt](#14-cicd-pour-dbt)
15. [Glossaire dbt](#15-glossaire-dbt)
16. [Erreurs courantes et debug](#16-erreurs-courantes-et-debug)
17. [Bonnes pratiques professionnelles](#17-bonnes-pratiques-professionnelles)
18. [Template de projet dbt réutilisable](#18-template-de-projet-dbt-réutilisable)
19. [Plan d'apprentissage dbt](#19-plan-dapprentissage-dbt)

---

## 1. Introduction à dbt

### 1.1 Qu'est-ce que dbt ?

**dbt** (data build tool) est un outil de transformation de données qui permet aux équipes data d'appliquer les bonnes pratiques du génie logiciel (versioning, tests, documentation, CI/CD) à leurs transformations SQL.

dbt se positionne dans la couche **T** (Transform) du pattern **ELT** (Extract, Load, Transform). Il ne gère ni l'extraction ni le chargement des données — il se concentre exclusivement sur la transformation des données déjà présentes dans votre data warehouse.

### 1.2 Ce que dbt fait

- Transforme les données avec du SQL et du Jinja
- Gère les dépendances entre modèles (DAG — Directed Acyclic Graph)
- Teste la qualité des données automatiquement
- Génère une documentation interactive
- Permet le versioning des transformations (Git)
- Offre un système de packages réutilisables
- Gère les matérialisations (views, tables, incrementals)
- S'intègre dans des pipelines CI/CD

### 1.3 Ce que dbt ne fait pas

- **Pas d'extraction** : dbt ne va pas chercher les données dans vos systèmes sources (utilisez Fivetran, Airbyte, Stitch, etc.)
- **Pas de chargement** : dbt ne charge pas de données brutes dans votre warehouse (sauf les seeds CSV)
- **Pas d'orchestration** : dbt ne planifie pas ses propres exécutions (utilisez Airflow, Dagster, Prefect, dbt Cloud, etc.)
- **Pas de visualisation** : dbt ne crée pas de dashboards (utilisez Looker, Metabase, Power BI, etc.)

### 1.4 Place dans une architecture data moderne

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Sources   │────▶│  Ingestion  │────▶│  Warehouse  │────▶│     BI /     │
│  (APIs, DB, │     │  (Fivetran, │     │ (BigQuery,  │     │  Analytics  │
│   Fichiers) │     │   Airbyte)  │     │  Snowflake, │     │  (Looker,   │
│             │     │             │     │  Redshift,  │     │  Metabase)  │
└─────────────┘     └─────────────┘     │  DuckDB...) │     └─────────────┘
                                        │             │
                                        │  ┌───────┐  │
                                        │  │  dbt  │  │
                                        │  │ (T de │  │
                                        │  │  ELT) │  │
                                        │  └───────┘  │
                                        └─────────────┘
```

### 1.5 dbt Core vs dbt Cloud

| Aspect | dbt Core | dbt Cloud |
|--------|----------|-----------|
| Prix | Gratuit (open source) | Payant (plan gratuit limité) |
| Exécution | Ligne de commande | Interface web + IDE |
| Orchestration | Externe (Airflow, etc.) | Intégrée (scheduler) |
| CI/CD | À configurer manuellement | Intégrée |
| Hébergement | Local / votre infra | Hébergé par dbt Labs |
| Documentation | `dbt docs serve` en local | Hébergée automatiquement |

> Ce guide couvre **dbt Core**. La quasi-totalité des concepts s'applique également à dbt Cloud.

---

## 2. Architecture d'un projet dbt

### 2.1 Structure complète d'un projet

```
mon_projet_dbt/
│
├── models/                     # Modèles SQL — cœur du projet
│   ├── staging/                # Couche 1 : nettoyage des sources
│   │   ├── stg_orders.sql
│   │   ├── stg_customers.sql
│   │   └── _stg_schema.yml    # Documentation et tests staging
│   │
│   ├── intermediate/           # Couche 2 : logique métier intermédiaire
│   │   ├── int_orders_enriched.sql
│   │   └── _int_schema.yml
│   │
│   └── marts/                  # Couche 3 : modèles prêts pour la BI
│       ├── finance/
│       │   ├── fct_revenue.sql
│       │   └── dim_customers.sql
│       ├── marketing/
│       │   └── fct_campaigns.sql
│       └── _marts_schema.yml
│
├── macros/                     # Fonctions Jinja réutilisables
│   ├── generate_schema_name.sql
│   └── cents_to_euros.sql
│
├── tests/                      # Tests singuliers (SQL custom)
│   └── assert_positive_revenue.sql
│
├── seeds/                      # Fichiers CSV chargés dans le warehouse
│   └── country_codes.csv
│
├── snapshots/                  # Historisation SCD Type 2
│   └── snapshot_customers.sql
│
├── analyses/                   # Requêtes ad hoc (non matérialisées)
│   └── monthly_revenue_check.sql
│
├── dbt_project.yml             # Configuration principale du projet
├── packages.yml                # Dépendances (packages dbt)
├── profiles.yml                # Connexion au warehouse (souvent hors du repo)
├── selectors.yml               # Groupes de sélection nommés (optionnel)
└── .gitignore                  # Fichiers à exclure de Git
```

### 2.2 Explication de chaque élément

#### `models/` — Le cœur du projet

Chaque fichier `.sql` dans ce dossier est un **modèle**. Un modèle est une instruction `SELECT` que dbt matérialise dans le warehouse (en tant que view, table, etc.).

La convention recommandée par dbt Labs organise les modèles en trois couches :

- **`staging/`** : un modèle par source. Renomme les colonnes, corrige les types, filtre les données invalides. Préfixe : `stg_`.
- **`intermediate/`** : combine et enrichit les modèles staging. Logique métier intermédiaire. Préfixe : `int_`.
- **`marts/`** : modèles finaux prêts pour la BI. Organisés par domaine métier. Préfixes : `fct_` (tables de faits) et `dim_` (tables de dimensions).

#### `macros/` — Fonctions réutilisables

Les macros sont des fonctions écrites en Jinja. Elles permettent de factoriser de la logique SQL répétitive. dbt fournit des macros intégrées et vous pouvez créer les vôtres.

#### `tests/` — Tests singuliers

Contient des requêtes SQL qui retournent les lignes qui échouent au test. Un test passe si la requête retourne 0 ligne.

#### `seeds/` — Données de référence

Fichiers CSV versionnés dans Git, chargés dans le warehouse via `dbt seed`. Idéal pour les données de mapping, les codes pays, les catégories.

#### `snapshots/` — Historisation

Capture les changements dans les données sources au fil du temps (Slowly Changing Dimensions Type 2).

#### `analyses/` — Requêtes non matérialisées

Requêtes SQL qui bénéficient du moteur Jinja de dbt mais qui ne sont pas matérialisées dans le warehouse. Utiles pour des analyses ponctuelles.

#### `dbt_project.yml` — Configuration du projet

Fichier racine qui définit le projet : nom, version, chemins des dossiers, configuration des matérialisations par défaut.

#### `profiles.yml` — Connexion au warehouse

Contient les informations de connexion au data warehouse. Ce fichier est généralement **en dehors du repo Git** (dans `~/.dbt/profiles.yml`) pour des raisons de sécurité.

#### `packages.yml` — Dépendances

Déclare les packages dbt externes à installer (comme `dbt_utils`, `dbt_expectations`).

---

## 3. Installation et démarrage

### 3.1 Installation de dbt Core

#### Option 1 : avec pip

```bash
# Créer un environnement virtuel (recommandé)
python3 -m venv .venv           # crée l'environnement virtuel
source .venv/bin/activate        # active l'environnement (Linux/Mac)
# .venv\Scripts\activate         # active l'environnement (Windows)

# Installer dbt avec l'adaptateur de votre warehouse
pip install dbt-bigquery         # pour Google BigQuery
pip install dbt-snowflake        # pour Snowflake
pip install dbt-postgres         # pour PostgreSQL
pip install dbt-redshift         # pour Amazon Redshift
pip install dbt-duckdb           # pour DuckDB (local, idéal pour apprendre)
```

#### Option 2 : avec uv (plus rapide)

```bash
# Installer uv (gestionnaire de packages ultra-rapide)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Initialiser un projet Python avec uv
uv init mon_projet
cd mon_projet
uv add dbt-bigquery              # ou dbt-snowflake, dbt-postgres, etc.
```

### 3.2 Initialiser un projet dbt

```bash
# Créer un nouveau projet dbt
dbt init mon_projet_dbt

# dbt va demander :
# 1. Le nom du projet
# 2. Le type de warehouse (bigquery, snowflake, postgres, etc.)
# 3. Les informations de connexion basiques
```

Cette commande crée toute l'arborescence du projet avec des fichiers d'exemple.

### 3.3 Configurer la connexion — `profiles.yml`

Le fichier `profiles.yml` se trouve par défaut dans `~/.dbt/profiles.yml`. Vous pouvez le placer à la racine du projet en définissant la variable d'environnement `DBT_PROFILES_DIR=.`.

#### Exemple pour BigQuery

```yaml
# profiles.yml — Connexion à Google BigQuery
mon_projet_dbt:                     # doit correspondre au champ 'profile' dans dbt_project.yml
  target: dev                        # environnement par défaut
  outputs:
    dev:                             # environnement de développement
      type: bigquery                 # type de warehouse
      method: oauth                  # méthode d'authentification
      project: mon-projet-gcp       # ID du projet GCP
      dataset: dbt_dev               # dataset (schema) de destination
      threads: 4                     # nombre de modèles en parallèle
      location: EU                   # localisation des données

    prod:                            # environnement de production
      type: bigquery
      method: service-account
      project: mon-projet-gcp-prod
      dataset: dbt_prod
      threads: 8
      keyfile: /chemin/vers/keyfile.json
      location: EU
```

#### Exemple pour Snowflake

```yaml
# profiles.yml — Connexion à Snowflake
mon_projet_dbt:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: mon-compte.eu-west-1  # identifiant du compte Snowflake
      user: "{{ env_var('SNOWFLAKE_USER') }}"       # variable d'environnement
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}" # ne jamais mettre en clair
      role: transformer              # rôle Snowflake
      database: analytics            # base de données
      warehouse: transforming_wh     # warehouse de calcul
      schema: dbt_dev                # schema de destination
      threads: 4
```

#### Exemple pour PostgreSQL

```yaml
# profiles.yml — Connexion à PostgreSQL
mon_projet_dbt:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost                # hôte du serveur
      port: 5432                     # port PostgreSQL
      user: dbt_user                 # utilisateur
      password: "{{ env_var('PG_PASSWORD') }}"  # mot de passe via variable d'env
      dbname: analytics              # nom de la base
      schema: dbt_dev                # schema de destination
      threads: 4
```

#### Exemple pour DuckDB (idéal pour apprendre)

```yaml
# profiles.yml — Connexion locale DuckDB
mon_projet_dbt:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: './output/warehouse.db'  # chemin vers le fichier de base de données
      schema: main                   # schema par défaut
```

### 3.4 Vérifier la connexion

```bash
# Vérifier que dbt peut se connecter au warehouse
dbt debug

# Sortie attendue :
# ✓ profiles.yml found
# ✓ dbt_project.yml found
# ✓ Connection test: OK
```

### 3.5 Première exécution

```bash
# 1. Installer les packages (si packages.yml existe)
dbt deps

# 2. Exécuter tous les modèles
dbt run

# 3. Lancer les tests
dbt test

# 4. Ou tout en une commande : run + test + seed + snapshot
dbt build
```

---

## 4. Concepts fondamentaux

### 4.1 Les modèles (models)

Un modèle dbt est un fichier `.sql` contenant une instruction `SELECT`. Lors de l'exécution, dbt enveloppe ce SELECT dans un DDL approprié (`CREATE VIEW`, `CREATE TABLE`, etc.) selon la matérialisation configurée.

```sql
-- models/staging/stg_orders.sql
-- Ce modèle nettoie et renomme les colonnes de la table source 'orders'

select
    id          as order_id,         -- renommage pour clarifier le nom
    user_id     as customer_id,      -- renommage : 'user_id' → 'customer_id'
    order_date,                      -- date conservée telle quelle
    status,                          -- statut de la commande
    amount / 100.0 as amount_euros   -- conversion des centimes en euros
from {{ source('ecommerce', 'orders') }}  -- référence la source déclarée
where status != 'cancelled'               -- exclut les commandes annulées
```

### 4.2 Les sources (sources)

Les sources déclarent les tables brutes déjà présentes dans votre warehouse. Elles sont définies dans un fichier YAML et référencées avec la fonction `{{ source() }}`.

```yaml
# models/staging/_sources.yml
version: 2

sources:
  - name: ecommerce                  # nom logique de la source
    description: "Base de données e-commerce de production"
    database: raw_database           # (optionnel) base de données
    schema: public                   # schema contenant les tables

    freshness:                       # vérification de fraîcheur (optionnel)
      warn_after:
        count: 12
        period: hour                 # alerte si données > 12h
      error_after:
        count: 24
        period: hour                 # erreur si données > 24h
    loaded_at_field: _loaded_at      # colonne de timestamp de chargement

    tables:
      - name: orders                 # nom réel de la table dans le warehouse
        description: "Table des commandes"
        columns:
          - name: id
            description: "Identifiant unique de la commande"
            tests:
              - unique
              - not_null

      - name: customers              # une autre table de la même source
        description: "Table des clients"
```

### 4.3 La fonction `{{ ref() }}`

`ref()` est la fonction la plus importante de dbt. Elle crée une **dépendance** entre deux modèles et permet à dbt de construire le DAG (graphe de dépendances).

```sql
-- models/marts/fct_revenue.sql
-- Ce modèle agrège le revenu par client
-- ref() garantit que stg_orders sera exécuté AVANT ce modèle

select
    customer_id,                             -- clé du client
    count(*) as total_orders,                -- nombre total de commandes
    sum(amount_euros) as total_revenue,      -- revenu total en euros
    min(order_date) as first_order_date,     -- date de la première commande
    max(order_date) as last_order_date       -- date de la dernière commande
from {{ ref('stg_orders') }}                 -- référence le modèle staging
group by customer_id                         -- agrégation par client
```

> **Règle d'or** : ne jamais écrire un nom de table en dur. Toujours utiliser `{{ ref() }}` pour les modèles et `{{ source() }}` pour les sources.

### 4.4 La fonction `{{ source() }}`

`source()` référence une table déclarée dans le fichier sources YAML. Elle garantit la traçabilité et active la vérification de fraîcheur.

```sql
-- Syntaxe : {{ source('nom_source', 'nom_table') }}
select *
from {{ source('ecommerce', 'orders') }}
-- dbt résout en : raw_database.public.orders (selon la config sources)
```

### 4.5 Le fichier `schema.yml`

Le fichier `schema.yml` (ou tout fichier `.yml` dans le dossier models) sert à documenter les modèles, déclarer des tests et configurer des propriétés.

```yaml
# models/staging/_stg_schema.yml
version: 2

models:
  - name: stg_orders                       # nom du modèle (sans .sql)
    description: "Commandes nettoyées et normalisées depuis la source e-commerce"
    columns:
      - name: order_id
        description: "Identifiant unique de la commande"
        tests:                             # tests automatiques sur cette colonne
          - unique                         # vérifie l'unicité
          - not_null                       # vérifie l'absence de NULL

      - name: customer_id
        description: "Identifiant du client ayant passé la commande"
        tests:
          - not_null
          - relationships:                 # vérifie l'intégrité référentielle
              to: ref('stg_customers')     # la table de référence
              field: customer_id           # le champ correspondant

      - name: amount_euros
        description: "Montant de la commande en euros"
        tests:
          - not_null
```

### 4.6 Variables

Les variables permettent de paramétrer vos modèles et macros.

```yaml
# dbt_project.yml — Déclaration de variables
vars:
  start_date: '2024-01-01'            # variable globale
  default_currency: 'EUR'             # accessible partout

  mon_projet_dbt:                      # variables spécifiques au projet
    tax_rate: 0.20                     # taux de TVA par défaut
```

```sql
-- Utilisation dans un modèle SQL
select *
from {{ ref('stg_orders') }}
where order_date >= '{{ var("start_date") }}'  -- utilise la variable start_date
```

### 4.7 Le DAG (Directed Acyclic Graph)

dbt construit automatiquement un graphe de dépendances à partir des fonctions `ref()` et `source()`. Ce graphe détermine l'ordre d'exécution des modèles.

```
sources.orders ──▶ stg_orders ──▶ int_orders_enriched ──▶ fct_revenue
                                                            │
sources.customers ──▶ stg_customers ──▶ dim_customers ◀────┘
```

Le DAG est visible dans la documentation générée (`dbt docs generate && dbt docs serve`).

---

## 5. Code commenté — Exemples complets

### 5.1 Exemple complet : modèle staging

```sql
-- models/staging/stg_orders.sql
-- =============================================================================
-- MODÈLE : stg_orders
-- COUCHE : Staging
-- SOURCE : ecommerce.orders
-- DESCRIPTION : Nettoie et normalise les commandes brutes
-- =============================================================================

-- Bloc de configuration dbt
-- 'view' est la matérialisation par défaut pour le staging (toujours à jour)
{{
  config(
    materialized='view',           -- matérialisé en tant que vue SQL
    tags=['staging', 'daily']      -- tags pour la sélection groupée
  )
}}

-- CTE 1 : extraction des données source
-- On isole la lecture de la source dans une CTE pour la clarté
with source_data as (

    select
        *                           -- on récupère toutes les colonnes
    from {{ source('ecommerce', 'orders') }}  -- table brute déclarée dans sources.yml

),

-- CTE 2 : renommage et typage des colonnes
-- Chaque colonne est explicitement nommée et documentée
renamed as (

    select
        id                          as order_id,          -- clé primaire
        user_id                     as customer_id,       -- clé étrangère vers clients
        cast(order_date as date)    as order_date,        -- conversion explicite en DATE
        status                      as order_status,      -- statut de la commande
        amount / 100.0              as amount_euros,      -- centimes → euros
        payment_type                as payment_method,    -- méthode de paiement
        created_at,                                       -- timestamp de création
        updated_at                                        -- timestamp de mise à jour
    from source_data

),

-- CTE 3 : filtrage des données invalides
-- On exclut les données incohérentes identifiées lors de l'analyse exploratoire
filtered as (

    select *
    from renamed
    where
        amount_euros > 0                              -- montant positif
        and order_date >= '2020-01-01'                -- pas de dates aberrantes
        and customer_id is not null                   -- client obligatoire
        and order_status != 'test'                    -- exclure les commandes test

)

-- Sélection finale
select * from filtered
```

### 5.2 Exemple complet : modèle intermediate

```sql
-- models/intermediate/int_orders_enriched.sql
-- =============================================================================
-- MODÈLE : int_orders_enriched
-- COUCHE : Intermediate
-- DESCRIPTION : Enrichit les commandes avec les données client
-- =============================================================================

{{
  config(
    materialized='view'            -- vue car couche intermédiaire
  )
}}

with orders as (

    select * from {{ ref('stg_orders') }}     -- commandes nettoyées

),

customers as (

    select * from {{ ref('stg_customers') }}  -- clients nettoyés

),

-- Jointure entre commandes et clients
enriched as (

    select
        o.order_id,                            -- identifiant de commande
        o.customer_id,                         -- identifiant client
        c.customer_name,                       -- nom du client (enrichissement)
        c.customer_segment,                    -- segment client (enrichissement)
        o.order_date,                          -- date de commande
        o.order_status,                        -- statut
        o.amount_euros,                        -- montant
        o.payment_method,                      -- méthode de paiement

        -- Calcul du rang chronologique des commandes par client
        row_number() over (
            partition by o.customer_id         -- par client
            order by o.order_date              -- trié par date
        ) as order_sequence_number             -- 1ère commande = 1, 2ème = 2, etc.

    from orders o
    left join customers c                      -- LEFT JOIN pour garder les commandes
        on o.customer_id = c.customer_id       -- même si le client est inconnu

)

select * from enriched
```

### 5.3 Exemple complet : modèle mart (table de faits)

```sql
-- models/marts/finance/fct_revenue.sql
-- =============================================================================
-- MODÈLE : fct_revenue
-- COUCHE : Marts (Finance)
-- DESCRIPTION : Table de faits du revenu par client, prête pour la BI
-- =============================================================================

{{
  config(
    materialized='table',          -- table car interrogé fréquemment par la BI
    tags=['finance', 'daily']      -- tags pour sélection et planification
  )
}}

with orders as (

    select * from {{ ref('int_orders_enriched') }}  -- commandes enrichies

),

-- Agrégation par client
revenue_by_customer as (

    select
        customer_id,                                 -- identifiant client
        customer_name,                               -- nom du client
        customer_segment,                            -- segment

        -- Métriques de revenu
        count(*)                    as total_orders,          -- nombre de commandes
        sum(amount_euros)           as lifetime_revenue,      -- revenu total
        avg(amount_euros)           as avg_order_value,       -- panier moyen

        -- Métriques temporelles
        min(order_date)             as first_order_date,      -- première commande
        max(order_date)             as last_order_date,       -- dernière commande
        datediff('day',
            min(order_date),
            max(order_date))        as customer_lifespan_days, -- durée de vie client

        -- Indicateur : client récurrent si plus d'une commande
        case
            when count(*) > 1 then true
            else false
        end                         as is_repeat_customer

    from orders
    where order_status = 'completed'                  -- uniquement les commandes livrées
    group by
        customer_id,
        customer_name,
        customer_segment

)

select * from revenue_by_customer
```

### 5.4 Exemple complet : modèle avec transformation de données brutes

Ce modèle est inspiré du code reconstruit à partir des documents sources. Il illustre un pipeline complet de nettoyage de données.

```sql
-- models/staging/stg_trips.sql
-- =============================================================================
-- MODÈLE : stg_trips
-- COUCHE : Staging
-- SOURCE : transport.raw_trips
-- DESCRIPTION : Nettoyage et transformation de données de trajets
-- =============================================================================

{{
  config(
    materialized='view'
  )
}}

-- CTE 1 : lecture de la source brute
with source_data as (

    select
        *
    from {{ source('transport', 'raw_trips') }}

),

-- CTE 2 : filtrage des données invalides
-- Ces règles de filtrage découlent de l'analyse exploratoire des données
filtered_data as (

    select *
    from source_data
    where
        passenger_count > 0                    -- au moins 1 passager
        and trip_distance > 0                  -- distance positive
        and total_amount > 0                   -- montant positif
        and pickup_datetime < dropoff_datetime -- cohérence temporelle
        and tip_amount >= 0                    -- pourboire non négatif

),

-- CTE 3 : transformation et enrichissement
transformed_data as (

    select
        -- Conversion de type pour cohérence
        cast(passenger_count as integer)    as passenger_count,

        -- Création d'un champ lisible à partir d'un code
        case
            when payment_type = 1 then 'Credit card'
            when payment_type = 2 then 'Cash'
            when payment_type = 3 then 'No charge'
            when payment_type = 4 then 'Dispute'
            else 'Unknown'
        end                                 as payment_method,

        -- Calcul de la durée du trajet en minutes
        datediff('minute',
            pickup_datetime,
            dropoff_datetime)               as trip_duration_minutes,

        -- Extraction de la date seule (pour le filtrage temporel)
        cast(pickup_datetime as date)       as pickup_date,
        cast(dropoff_datetime as date)      as dropoff_date,

        -- Colonnes conservées telles quelles
        pickup_datetime,
        dropoff_datetime,
        trip_distance,
        pickup_location_id,
        dropoff_location_id,
        fare_amount,
        tip_amount,
        tolls_amount,
        total_amount

    from filtered_data

),

-- CTE 4 : filtrage final sur la période souhaitée
final_data as (

    select *
    from transformed_data
    where
        pickup_date >= '{{ var("start_date", "2024-01-01") }}'   -- date de début (variable)
        and pickup_date < '{{ var("end_date", "2025-01-01") }}'  -- date de fin (variable)
        and trip_duration_minutes > 0                              -- durée positive

)

-- Sélection finale sans les colonnes de travail intermédiaires
select
    pickup_datetime,
    dropoff_datetime,
    trip_duration_minutes,
    passenger_count,
    trip_distance,
    payment_method,
    pickup_location_id,
    dropoff_location_id,
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount
from final_data
```

---

## 6. Matérialisations — Guide complet

### 6.1 Vue d'ensemble

dbt propose 5 types de matérialisation natifs :

| Type | SQL généré | Stockage | Mise à jour | Cas d'usage |
|------|-----------|----------|-------------|-------------|
| **view** | `CREATE VIEW` | Aucun | Toujours à jour | Staging, modèles légers |
| **table** | `CREATE TABLE` | Complet | Recréée à chaque run | Marts, BI, petites tables |
| **incremental** | `INSERT` / `MERGE` | Complet | Seules les nouvelles données | Gros volumes, logs, événements |
| **ephemeral** | CTE (pas de DDL) | Aucun | Inline à chaque utilisation | Logique réutilisable sans créer d'objet |
| **materialized_view** | `CREATE MATERIALIZED VIEW` | Complet | Auto-refresh par le warehouse | Requêtes fréquentes, freshness automatique |

### 6.2 View (par défaut)

```sql
-- Matérialisation par défaut — pas besoin de config()
-- Ou explicitement :
{{ config(materialized='view') }}

select
    order_id,
    customer_id,
    amount_euros
from {{ source('ecommerce', 'orders') }}
```

**Avantages** : aucune donnée stockée, toujours à jour.
**Inconvénients** : lent si empilé sur plusieurs niveaux de vues.
**Usage** : staging, modèles intermédiaires légers.

### 6.3 Table

```sql
{{ config(materialized='table') }}

select
    customer_id,
    count(*) as total_orders,
    sum(amount_euros) as total_revenue
from {{ ref('stg_orders') }}
group by customer_id
```

**Avantages** : rapide à requêter (données pré-calculées).
**Inconvénients** : recréée entièrement à chaque `dbt run` (lent sur gros volumes), coût de stockage.
**Usage** : marts, tables de faits/dimensions interrogées par la BI.

### 6.4 Incremental

Le modèle incrémental ne traite que les **nouvelles données** à chaque exécution, au lieu de tout recalculer.

```sql
-- models/marts/fct_events.sql
{{
  config(
    materialized='incremental',           -- matérialisation incrémentale
    unique_key='event_id',                -- clé pour identifier les doublons
    incremental_strategy='merge',         -- stratégie : merge, append, delete+insert
    on_schema_change='append_new_columns' -- gestion des nouvelles colonnes
  )
}}

select
    event_id,                              -- identifiant unique de l'événement
    user_id,                               -- identifiant utilisateur
    event_type,                            -- type d'événement (click, purchase, etc.)
    event_timestamp,                       -- horodatage de l'événement
    event_data                             -- données supplémentaires
from {{ source('analytics', 'raw_events') }}

-- Ce bloc conditionnel est la clé de l'incrémentalité
-- Il n'est actif que lors des exécutions APRÈS la première (full refresh)
{% if is_incremental() %}
    -- Ne traiter que les événements plus récents que le dernier traitement
    where event_timestamp > (
        select max(event_timestamp)        -- le timestamp le plus récent
        from {{ this }}                    -- {{ this }} = la table actuelle
    )
{% endif %}
```

#### Les 5 stratégies incrémentales

**1. Append** — Ajoute sans vérifier les doublons

```sql
{{ config(materialized='incremental', incremental_strategy='append') }}
-- Risque de doublons. Adapté aux logs immutables (jamais mis à jour).
```

**2. Merge** — Insère ou met à jour selon la `unique_key`

```sql
{{ config(materialized='incremental', incremental_strategy='merge', unique_key='event_id') }}
-- Le warehouse scanne la table existante pour trouver les correspondances.
-- Disponible : BigQuery, Snowflake, Databricks, Spark.
```

**3. Delete+Insert** — Supprime les lignes existantes, puis insère

```sql
{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='event_id') }}
-- Supprime d'abord les lignes dont la unique_key existe dans les nouvelles données,
-- puis insère toutes les nouvelles lignes.
-- Disponible : Snowflake, Redshift, Postgres.
```

**4. Insert_overwrite** — Remplace des partitions entières

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'event_date', 'data_type': 'date'}  -- BigQuery
) }}
-- Supprime et recrée les partitions affectées par les nouvelles données.
-- Très performant sur BigQuery et Spark.
```

**5. Microbatch** — Découpe le travail en lots temporels

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='microbatch',
    unique_key='event_id',
    event_time='event_timestamp',        -- colonne temporelle
    begin='2024-01-01',                  -- date de début pour le full refresh
    batch_size='day'                     -- granularité : hour, day, month, year
) }}

select
    event_id,
    user_id,
    event_type,
    event_timestamp
from {{ source('analytics', 'raw_events') }}
-- Pas besoin de {% if is_incremental() %} — dbt le gère automatiquement
-- dbt découpe le traitement en lots (1 jour à la fois)
```

### 6.5 Ephemeral

```sql
{{ config(materialized='ephemeral') }}

-- Ce modèle ne crée aucun objet dans le warehouse
-- Il est injecté comme une CTE dans les modèles qui l'appellent avec ref()

select
    id as country_id,
    name as country_name,
    iso_code
from {{ source('reference', 'countries') }}
```

**Avantages** : pas d'objet créé, réduit l'encombrement du warehouse.
**Inconvénients** : impossible de le requêter directement, complique le debug.
**Usage** : petites transformations réutilisées par plusieurs modèles.

### 6.6 Materialized View

```sql
{{ config(materialized='materialized_view') }}

select
    customer_segment,
    count(*) as customer_count,
    avg(lifetime_revenue) as avg_revenue
from {{ ref('fct_revenue') }}
group by customer_segment
```

**Avantages** : performance de requête comme une table, fraîcheur gérée par le warehouse.
**Inconvénients** : non supporté par tous les warehouses, options de configuration limitées.
**Usage** : agrégations fréquemment consultées.

### 6.7 Arbre de décision : quelle matérialisation choisir ?

```
                    ┌─────────────────────────────┐
                    │  Le modèle sera interrogé   │
                    │   par des outils BI ?       │
                    └──────────┬──────────────────┘
                               │
                    ┌──── Oui ─┴─ Non ────┐
                    │                     │
                    ▼                     ▼
        ┌───────────────────┐   ┌────────────────────┐
        │ TABLE,            │   │ Beaucoup de         │
        │ MATERIALIZED VIEW │   │ transformations ?   │
        │ ou INCREMENTAL    │   └───────┬────────────┘
        └───────────────────┘           │
                                 ┌─ Oui ┴─ Non ─┐
                                 │               │
                                 ▼               ▼
                         ┌──────────────┐ ┌──────────────┐
                         │ TABLE ou     │ │ VIEW ou      │
                         │ INCREMENTAL  │ │ EPHEMERAL    │
                         └──────────────┘ └──────┬───────┘
                                                 │
                                    ┌── Le build est ──┐
                                    │   trop lent ?     │
                                    └───────┬──────────┘
                                     ┌─ Oui ┴─ Non ─┐
                                     │               │
                                     ▼               ▼
                             ┌──────────────┐ ┌──────────┐
                             │ INCREMENTAL  │ │ Continuer│
                             └──────────────┘ │ tel quel │
                                              └──────────┘
```

---

## 7. Tests et qualité des données

### 7.1 Tests génériques natifs

dbt fournit 4 tests génériques intégrés :

```yaml
# models/staging/_stg_schema.yml
version: 2

models:
  - name: stg_orders
    columns:
      - name: order_id
        tests:
          - unique                     # aucun doublon dans cette colonne
          - not_null                   # aucune valeur NULL

      - name: order_status
        tests:
          - accepted_values:           # uniquement les valeurs autorisées
              values:
                - 'pending'
                - 'shipped'
                - 'completed'
                - 'returned'

      - name: customer_id
        tests:
          - not_null
          - relationships:             # intégrité référentielle
              to: ref('stg_customers') # table de référence
              field: customer_id       # colonne correspondante
```

### 7.2 Tests avec le package `dbt_expectations`

Le package `dbt_expectations` offre des dizaines de tests supplémentaires inspirés de Great Expectations.

```yaml
# packages.yml
packages:
  - package: calogica/dbt_expectations
    version: [">=0.10.0", "<0.11.0"]    # contrainte de version
```

```yaml
# models/staging/_stg_schema.yml
version: 2

models:
  - name: stg_orders
    columns:
      - name: amount_euros
        tests:
          - not_null
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0             # montant minimum attendu
              max_value: 100000        # montant maximum raisonnable

      - name: passenger_count
        tests:
          - dbt_expectations.expect_column_values_to_be_of_type:
              column_type: integer     # vérification du type de données

      - name: order_date
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: "'2020-01-01'"  # pas de date avant 2020
              max_value: "current_date"  # pas de date dans le futur
```

### 7.3 Tests singuliers (SQL custom)

Les tests singuliers sont des fichiers SQL dans le dossier `tests/`. La requête doit retourner les lignes qui **échouent**. Si 0 ligne est retournée, le test passe.

```sql
-- tests/assert_positive_revenue.sql
-- Ce test vérifie que le revenu total n'est jamais négatif
-- Il retourne les clients avec un revenu négatif (= échecs)

select
    customer_id,                  -- identifiant du client en erreur
    lifetime_revenue              -- montant négatif trouvé
from {{ ref('fct_revenue') }}     -- table de faits à tester
where lifetime_revenue < 0        -- condition d'échec
```

```sql
-- tests/assert_twelve_months.sql
-- Ce test vérifie que les données couvrent bien 12 mois
-- Il échoue si le nombre de mois distincts n'est pas 12

with months as (
    select distinct
        extract(month from pickup_datetime) as month_number  -- extraction du mois
    from {{ ref('stg_trips') }}                               -- modèle à tester
)
select count(*) as month_count                                -- nombre de mois distincts
from months
having count(*) <> 12                                         -- échoue si ≠ 12
```

### 7.4 Configuration avancée des tests

```yaml
# Configurer la sévérité et le comportement des tests
models:
  - name: stg_orders
    columns:
      - name: amount_euros
        tests:
          - not_null:
              severity: warn           # avertissement au lieu d'erreur
          - accepted_values:
              values: ['Credit card', 'Cash']
              severity: error          # bloque le pipeline si échoue
              error_if: ">100"         # erreur si plus de 100 lignes échouent
              warn_if: ">10"           # warning si plus de 10 lignes échouent
              where: "order_date >= '2024-01-01'"  # filtre appliqué au test
```

---

## 8. Jinja et Macros

### 8.1 Syntaxe Jinja dans dbt

Jinja est le moteur de templating intégré à dbt. Il enrichit le SQL avec de la logique dynamique.

```sql
-- Les 3 types de blocs Jinja :

-- 1. Expressions : {{ ... }} → affichent une valeur
select * from {{ ref('stg_orders') }}

-- 2. Instructions : {% ... %} → logique (if, for, set)
{% if target.name == 'dev' %}
    limit 100                            -- limite en dev pour accélérer le dev
{% endif %}

-- 3. Commentaires : {# ... #} → ignorés à la compilation
{# Ce commentaire n'apparaît pas dans le SQL compilé #}
```

### 8.2 Conditions et boucles

```sql
-- Exemple de condition : adapter le comportement selon l'environnement
select *
from {{ ref('stg_orders') }}
{% if target.name == 'dev' %}
    where order_date >= dateadd('month', -3, current_date)  -- 3 mois en dev
{% elif target.name == 'staging' %}
    where order_date >= dateadd('year', -1, current_date)   -- 1 an en staging
{% endif %}
-- En prod, pas de filtre → toutes les données
```

```sql
-- Exemple de boucle : générer dynamiquement du SQL
{% set payment_methods = ['credit_card', 'cash', 'bank_transfer'] %}

select
    order_id,
    {% for method in payment_methods %}
        sum(case when payment_method = '{{ method }}' then amount else 0 end)
            as {{ method }}_amount                    -- colonne par méthode
        {% if not loop.last %},{% endif %}            -- virgule sauf après le dernier
    {% endfor %}
from {{ ref('stg_payments') }}
group by order_id
```

### 8.3 Créer une macro

```sql
-- macros/cents_to_euros.sql
-- Macro utilitaire pour convertir les centimes en euros

{% macro cents_to_euros(column_name, precision=2) %}
    -- Divise par 100 et arrondit au nombre de décimales souhaité
    round({{ column_name }} / 100.0, {{ precision }})
{% endmacro %}
```

```sql
-- Utilisation dans un modèle :
select
    order_id,
    {{ cents_to_euros('amount_cents') }} as amount_euros,     -- appel de la macro
    {{ cents_to_euros('tax_cents', 4) }} as tax_euros          -- avec précision custom
from {{ source('ecommerce', 'raw_orders') }}
```

### 8.4 Macro : générer un schema dynamique

```sql
-- macros/generate_schema_name.sql
-- Surcharge la macro native pour personnaliser les noms de schemas

{% macro generate_schema_name(custom_schema_name, node) %}
    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}                              -- pas de custom → schema par défaut
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}                   -- en prod → utilise le custom tel quel
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}  -- en dev → préfixe avec le schema dev
    {%- endif -%}
{% endmacro %}
```

### 8.5 Macro utile : limit en CI

```sql
-- macros/limit_for_ci.sql
-- Macro pour limiter les données en environnement CI

{% macro limit_for_ci() %}
    {%- if var('is_ci', false) -%}
        limit 1000                    -- échantillon réduit en CI
    {%- endif -%}
{% endmacro %}
```

```sql
-- Utilisation dans un modèle :
select *
from {{ ref('stg_orders') }}
{{ limit_for_ci() }}                  -- ajoute 'limit 1000' seulement en CI
```

---

## 9. Packages dbt

### 9.1 Déclarer des packages

```yaml
# packages.yml — à la racine du projet
packages:
  # Packages depuis le hub dbt (packages.getdbt.com)
  - package: dbt-labs/dbt_utils             # utilitaires courants
    version: [">=1.0.0", "<2.0.0"]

  - package: calogica/dbt_expectations      # tests avancés (style Great Expectations)
    version: [">=0.10.0", "<0.11.0"]

  - package: dbt-labs/codegen               # génération de code (sources, staging)
    version: [">=0.12.0", "<0.13.0"]

  - package: dbt-labs/audit_helper           # comparaison de données entre modèles
    version: [">=0.9.0", "<0.10.0"]

  # Package depuis un dépôt Git
  - git: "https://github.com/org/mon-package.git"
    revision: v1.0.0                         # tag, branche ou commit
```

```bash
# Installer les packages après avoir modifié packages.yml
dbt deps
```

### 9.2 Packages essentiels

| Package | Utilité | Exemples de macros/tests |
|---------|---------|--------------------------|
| `dbt_utils` | Boîte à outils générale | `surrogate_key`, `pivot`, `unpivot`, `date_spine` |
| `dbt_expectations` | Tests avancés | `expect_column_to_exist`, `expect_values_to_be_between` |
| `codegen` | Génération automatique | `generate_source`, `generate_model_yaml` |
| `audit_helper` | Audit et comparaison | `compare_relations`, `compare_column_values` |
| `dbt_date` | Utilitaires date | `get_date_dimension`, `day_of_week` |

### 9.3 Utilisation de dbt_utils — exemples

```sql
-- Générer une clé de substitution (surrogate key)
select
    {{ dbt_utils.generate_surrogate_key(['order_id', 'product_id']) }} as order_line_id,
    order_id,
    product_id,
    quantity
from {{ ref('stg_order_lines') }}
```

```sql
-- Pivoter des lignes en colonnes
select
    order_id,
    {{ dbt_utils.pivot(
        column='payment_method',
        values=['credit_card', 'cash', 'bank_transfer'],
        agg='sum',
        then_value='amount'
    ) }}
from {{ ref('stg_payments') }}
group by order_id
```

---

## 10. Seeds, Snapshots et Analyses

### 10.1 Seeds — Charger des fichiers CSV

Les seeds sont des fichiers CSV versionnés dans Git, chargés dans le warehouse.

```csv
# seeds/country_codes.csv
code,name,region
FR,France,Europe
DE,Germany,Europe
US,United States,North America
JP,Japan,Asia
BR,Brazil,South America
```

```bash
# Charger tous les seeds dans le warehouse
dbt seed

# Charger un seed spécifique
dbt seed --select country_codes
```

```sql
-- Utiliser un seed comme n'importe quel modèle via ref()
select
    o.order_id,
    c.name as country_name,         -- enrichissement avec le seed
    c.region                        -- région depuis le seed
from {{ ref('stg_orders') }} o
left join {{ ref('country_codes') }} c  -- référence au seed
    on o.country_code = c.code
```

> **Usage recommandé** : tables de mapping, codes de référence, petites tables de lookup (< quelques milliers de lignes). Ne pas utiliser pour de gros volumes de données.

### 10.2 Snapshots — Historisation SCD Type 2

Les snapshots capturent les changements au fil du temps en créant un historique.

```sql
-- snapshots/snapshot_customers.sql

{% snapshot snapshot_customers %}

{{
  config(
    target_schema='snapshots',             -- schema de destination
    unique_key='customer_id',              -- clé pour identifier chaque ligne
    strategy='timestamp',                  -- stratégie basée sur un timestamp
    updated_at='updated_at',               -- colonne de dernière mise à jour
    invalidate_hard_deletes=true           -- gérer les suppressions
  )
}}

-- La requête capture l'état actuel de la table source
select
    customer_id,                            -- clé primaire
    customer_name,                          -- nom (peut changer)
    customer_email,                         -- email (peut changer)
    customer_segment,                       -- segment (peut changer)
    updated_at                              -- timestamp de mise à jour
from {{ source('ecommerce', 'customers') }}

{% endsnapshot %}
```

```bash
# Exécuter les snapshots
dbt snapshot
```

Résultat dans le warehouse : dbt ajoute automatiquement les colonnes `dbt_valid_from`, `dbt_valid_to` et `dbt_scd_id` pour tracer l'historique.

### 10.3 Analyses — Requêtes non matérialisées

Les analyses sont des fichiers SQL qui bénéficient du moteur Jinja mais ne créent aucun objet dans le warehouse. Elles sont compilées par `dbt compile` et le résultat se trouve dans `target/compiled/`.

```sql
-- analyses/monthly_revenue_check.sql
-- Requête d'analyse ponctuelle — non matérialisée

select
    date_trunc('month', order_date) as month,
    count(*) as order_count,
    sum(amount_euros) as total_revenue
from {{ ref('stg_orders') }}           -- bénéficie de ref() et du Jinja
group by date_trunc('month', order_date)
order by month desc
```

---

## 11. Documentation du projet

### 11.1 Documentation des modèles et colonnes

La documentation se fait directement dans les fichiers YAML :

```yaml
# models/marts/_marts_schema.yml
version: 2

models:
  - name: fct_revenue
    description: >
      Table de faits contenant les métriques de revenu par client.
      Alimentée quotidiennement à partir des commandes terminées.
      Utilisée par les dashboards Finance et Marketing.

    meta:                                   # métadonnées custom
      owner: "equipe-data"
      contains_pii: false
      refresh_frequency: "daily"

    columns:
      - name: customer_id
        description: "Identifiant unique du client"
      - name: lifetime_revenue
        description: >
          Somme de tous les montants des commandes terminées pour ce client.
          Exprimé en euros.
      - name: is_repeat_customer
        description: "Booléen : true si le client a passé plus d'une commande"
```

### 11.2 Blocs `{% docs %}` personnalisés

```markdown
-- models/docs.md
{% docs payment_method %}

Méthode de paiement utilisée pour la commande.

| Valeur | Description |
|--------|-------------|
| Credit card | Paiement par carte bancaire |
| Cash | Paiement en espèces |
| Bank transfer | Virement bancaire |

{% enddocs %}
```

```yaml
# Référencer le bloc docs dans le schema
columns:
  - name: payment_method
    description: "{{ doc('payment_method') }}"   # utilise le bloc docs
```

### 11.3 Générer et consulter la documentation

```bash
# Générer les fichiers de documentation
dbt docs generate

# Lancer le serveur de documentation en local
dbt docs serve
# → Ouvre un navigateur sur http://localhost:8080
# → Inclut le graphe de lignée (lineage graph) interactif
```

---

## 12. Commandes dbt — Référence complète

### `dbt init`

```bash
# Créer un nouveau projet dbt
dbt init mon_projet

# Syntaxe
dbt init <nom_du_projet>
```

**Rôle** : initialise un nouveau projet dbt avec l'arborescence complète et des fichiers d'exemple.
**Cas d'utilisation** : démarrage d'un nouveau projet.

---

### `dbt debug`

```bash
# Vérifier la connexion et la configuration
dbt debug
```

**Rôle** : vérifie que `profiles.yml` et `dbt_project.yml` sont valides, et que la connexion au warehouse fonctionne.
**Cas d'utilisation** : diagnostic des problèmes de connexion, validation initiale.

---

### `dbt deps`

```bash
# Installer les packages déclarés dans packages.yml
dbt deps
```

**Rôle** : télécharge et installe les packages dbt déclarés dans `packages.yml`.
**Cas d'utilisation** : après modification de `packages.yml`, setup initial.

---

### `dbt compile`

```bash
# Compiler les modèles sans les exécuter
dbt compile

# Compiler un modèle spécifique
dbt compile --select stg_orders
```

**Rôle** : traduit le Jinja en SQL pur sans exécuter quoi que ce soit. Le SQL compilé est écrit dans `target/compiled/`.
**Cas d'utilisation** : debug du Jinja, vérification du SQL généré, étape CI.

---

### `dbt run`

```bash
# Exécuter tous les modèles
dbt run

# Exécuter un modèle et ses dépendances
dbt run --select +fct_revenue

# Exécuter uniquement les modèles modifiés (Slim CI)
dbt run --select state:modified --defer --state ./prod_manifest/

# Full refresh d'un modèle incrémental (recréer de zéro)
dbt run --select fct_events --full-refresh
```

**Rôle** : exécute les modèles sélectionnés dans le warehouse.
**Cas d'utilisation** : exécution quotidienne, développement, déploiement.

---

### `dbt test`

```bash
# Lancer tous les tests
dbt test

# Tester un modèle spécifique
dbt test --select stg_orders

# Tester uniquement les tests singuliers
dbt test --select test_type:singular

# Tester avec un seuil de sévérité
dbt test --select stg_orders --severity warn
```

**Rôle** : exécute les tests définis dans les fichiers YAML et dans le dossier `tests/`.
**Cas d'utilisation** : validation de la qualité des données, CI/CD.

---

### `dbt build`

```bash
# Exécuter modèles + tests + seeds + snapshots dans l'ordre du DAG
dbt build

# Build sélectif
dbt build --select +fct_revenue

# Build uniquement ce qui a changé
dbt build --select 1+state:modified+1 --defer --state ./prod_manifest/
```

**Rôle** : combine `dbt run`, `dbt test`, `dbt seed` et `dbt snapshot` en une seule commande, en respectant l'ordre du DAG (les tests sont exécutés juste après le modèle correspondant).
**Cas d'utilisation** : exécution complète en production, CI/CD.

---

### `dbt seed`

```bash
# Charger tous les fichiers CSV dans le warehouse
dbt seed

# Charger un seed spécifique
dbt seed --select country_codes

# Forcer le rechargement complet
dbt seed --full-refresh
```

**Rôle** : charge les fichiers CSV du dossier `seeds/` dans le warehouse.
**Cas d'utilisation** : tables de référence, données de mapping.

---

### `dbt snapshot`

```bash
# Exécuter tous les snapshots
dbt snapshot

# Exécuter un snapshot spécifique
dbt snapshot --select snapshot_customers
```

**Rôle** : capture l'état actuel des données sources pour l'historisation (SCD Type 2).
**Cas d'utilisation** : tracking des changements de données dans le temps.

---

### `dbt docs generate` / `dbt docs serve`

```bash
# Générer la documentation
dbt docs generate

# Servir la documentation en local
dbt docs serve
# → http://localhost:8080
```

**Rôle** : génère une documentation HTML interactive incluant le graphe de lignée.
**Cas d'utilisation** : documentation d'équipe, onboarding, data discovery.

---

### `dbt list` (ou `dbt ls`)

```bash
# Lister toutes les ressources du projet
dbt ls

# Lister uniquement les modèles
dbt ls --resource-type model

# Lister les modèles modifiés
dbt ls --select state:modified --state ./prod_manifest/

# Lister en format JSON
dbt ls --output json
```

**Rôle** : liste les ressources du projet sans les exécuter.
**Cas d'utilisation** : exploration, vérification de sélecteurs, scripts CI.

---

### `dbt clean`

```bash
# Nettoyer les artefacts de compilation
dbt clean
```

**Rôle** : supprime les dossiers `target/` et `dbt_packages/` (configurables via `clean-targets` dans `dbt_project.yml`).
**Cas d'utilisation** : réinitialiser l'environnement local, résoudre des problèmes de cache.

---

### `dbt run-operation`

```bash
# Exécuter une macro directement
dbt run-operation drop_ci_schemas

# Avec des arguments
dbt run-operation grant_select --args '{"schema": "analytics", "role": "reader"}'
```

**Rôle** : exécute une macro en dehors d'un modèle.
**Cas d'utilisation** : opérations de maintenance (nettoyage, permissions), scripts CI.

---

### `dbt source freshness`

```bash
# Vérifier la fraîcheur de toutes les sources
dbt source freshness

# Vérifier une source spécifique
dbt source freshness --select source:ecommerce
```

**Rôle** : vérifie que les données sources sont à jour selon la configuration `freshness` dans `sources.yml`.
**Cas d'utilisation** : monitoring des pipelines d'ingestion, alerting.

---

### `dbt retry`

```bash
# Relancer uniquement les nœuds qui ont échoué au dernier run
dbt retry
```

**Rôle** : relance les modèles/tests qui ont échoué lors de la dernière exécution.
**Cas d'utilisation** : correction rapide après un échec partiel.

---

## 13. Sélection de nœuds — Node Selection

### 13.1 Méthodes de sélection

| Méthode | Syntaxe | Exemple |
|---------|---------|---------|
| Par nom | `model_name` | `dbt run --select stg_orders` |
| Par tag | `tag:value` | `dbt run --select tag:daily` |
| Par source | `source:name` | `dbt run --select source:ecommerce` |
| Par type | `resource_type:type` | `dbt ls --select resource_type:test` |
| Par chemin | `path:folder/` | `dbt run --select path:models/staging` |
| Par fichier | `file:name.sql` | `dbt run --select file:stg_orders.sql` |
| Par config | `config.key:value` | `dbt run --select config.materialized:table` |
| Par état | `state:modified` | `dbt run --select state:modified` |
| Par résultat | `result:error` | `dbt run --select result:error` |
| Par package | `package:name` | `dbt run --select package:dbt_utils` |
| Par test type | `test_type:singular` | `dbt test --select test_type:singular` |
| Par accès | `access:public` | `dbt ls --select access:public` |
| Par groupe | `group:finance` | `dbt run --select group:finance` |

### 13.2 Graph operators

Les graph operators permettent de sélectionner les parents et enfants d'un modèle dans le DAG.

```bash
# Le modèle et tous ses enfants (downstream)
dbt run --select "stg_orders+"

# Le modèle et ses 2 premiers enfants
dbt run --select "stg_orders+2"

# Tous les parents (upstream) et le modèle
dbt run --select "+fct_revenue"

# Les 3 parents les plus proches et le modèle
dbt run --select "3+fct_revenue"

# Le modèle et toutes ses dépendances (parents + enfants)
dbt run --select "@fct_revenue"

# Combinaison : 1 parent + le modèle modifié + 1 enfant (Slim CI)
dbt build --select "1+state:modified+1"
```

### 13.3 Combinaisons

```bash
# Union (ou) : modèles du staging OU tagués 'daily'
dbt run --select "path:models/staging tag:daily"

# Intersection (et) : modèles dans staging ET tagués 'critical'
dbt run --select "path:models/staging,tag:critical"

# Exclusion : tout sauf le modèle debug
dbt run --exclude "stg_debug"

# Combinaison complexe
dbt run --select "path:models/marts" --exclude "tag:experimental"
```

### 13.4 Selectors YAML

Pour les sélections complexes et réutilisables, créez un fichier `selectors.yml` :

```yaml
# selectors.yml
selectors:
  - name: nightly_build                     # nom du sélecteur
    description: "Modèles à exécuter chaque nuit"
    definition:
      union:                                # combine plusieurs critères
        - intersection:                     # ET logique
            - "tag:nightly"                 # tagués 'nightly'
            - "path:models/marts"           # dans le dossier marts
        - "path:models/staging"             # OU tout le staging
        - exclude:                          # SAUF
            - "config.materialized:ephemeral" # les modèles éphémères
```

```bash
# Utiliser un sélecteur nommé
dbt build --selector nightly_build
```

---

## 14. CI/CD pour dbt

### 14.1 Principes d'un bon CI/CD dbt

Un bon pipeline CI/CD pour dbt doit :

1. **Tester la compilation** avant tout (rapide, attrape 80% des bugs)
2. **Vérifier la qualité du code** (linting SQL, YAML)
3. **Builder uniquement ce qui a changé** (Slim CI)
4. **Travailler sur un échantillon de données** (pas les données de production complètes)
5. **Automatiser le déploiement** (pas de clic manuel en production)
6. **Notifier l'équipe** en cas de succès ou d'échec

### 14.2 Erreurs courantes

| Erreur | Conséquence | Solution |
|--------|-------------|----------|
| `dbt build` complet à chaque PR | CI lente (45+ min), coûteuse | Slim CI avec `state:modified` |
| Build sur données prod complètes | Coûts warehouse élevés | Échantillonner les données en CI |
| Pas de linting SQL | Code inconsistant, bugs | SQLFluff + yamllint |
| Déploiement manuel | Erreurs humaines, lenteur | CD automatisé via GitHub Actions |
| Pas de rollback | Blocage en cas de bug en prod | Tags Git + redéploiement automatique |

### 14.3 Pipeline CI complet (GitHub Actions)

```yaml
# .github/workflows/ci.yml
name: CI dbt

on:
  pull_request:                           # déclenché sur chaque PR
    branches:
      - main
      - preprod

env:
  DBT_PROFILES_DIR: .                      # profiles.yml à la racine du projet
  DBT_TARGET: ci                           # target spécifique pour la CI

jobs:
  ci:
    name: CI
    runs-on: ubuntu-latest

    steps:
      # ───────────────────────────────────
      # 1. SETUP — Préparation de l'env.
      # ───────────────────────────────────
      - name: Checkout code                # récupère le code de la PR
        uses: actions/checkout@v4

      - name: Set up Python               # installe Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install uv                  # uv = gestionnaire de packages rapide
        uses: astral-sh/setup-uv@v4
        with:
          version: "latest"
          enable-cache: true               # cache pour accélérer les builds suivants

      - name: Install dependencies         # installe dbt et ses dépendances
        run: uv sync --frozen

      - name: Install dbt packages         # installe les packages dbt (dbt_utils, etc.)
        run: uv run dbt deps

      # ───────────────────────────────────
      # 2. QUALITÉ DU CODE
      # ───────────────────────────────────
      - name: Lint YAML files              # vérifie la syntaxe des fichiers YAML
        run: uv run yamllint models/

      - name: Score dbt project quality    # évalue la qualité globale du projet
        run: uv run dbt-score lint
        # Vérifie par modèle :
        # - présence d'une description
        # - documentation des colonnes
        # - présence de tests
        # - utilisation de ref() et source() (pas de noms en dur)

      - name: Lint SQL with SQLFluff       # vérifie le style SQL
        run: |
          uv run sqlfluff lint models/ \
            --dialect bigquery \            # adapter selon votre warehouse
            --templater dbt \              # utilise le templater dbt pour le Jinja
            --processes 4                  # parallélisme pour accélérer

      # ───────────────────────────────────
      # 3. COMPILATION
      # ───────────────────────────────────
      - name: Validate dbt compilation     # compile le Jinja → SQL sans exécuter
        run: uv run dbt compile
        env:
          DBT_GOOGLE_PROJECT: ${{ secrets.DBT_GOOGLE_PROJECT }}
          DBT_GOOGLE_KEYFILE: ${{ secrets.DBT_GOOGLE_KEYFILE }}

      # ───────────────────────────────────
      # 4. MANIFEST DE PRODUCTION
      # ───────────────────────────────────
      - name: Generate production manifest # récupère l'état actuel de la prod
        run: |
          uv run dbt ls \
            --target prod \
            --state prod_manifest/
          mkdir -p prod_manifest
          cp target/manifest.json prod_manifest/manifest.json
        env:
          DBT_TARGET: prod
        continue-on-error: true            # OK si c'est le premier run (pas encore de prod)

      # ───────────────────────────────────
      # 5. SLIM CI — Build sélectif
      # ───────────────────────────────────
      - name: Build modified models        # ne build que ce qui a changé
        run: |
          uv run dbt build \
            --select 1+state:modified+1 \  # 1 parent + modifié + 1 enfant
            --defer \                       # utilise la prod pour les modèles non modifiés
            --state prod_manifest/ \        # chemin vers le manifest de prod
            --vars '{"is_ci": true}'        # active le mode CI (ex: limit_for_ci)
        env:
          DBT_GOOGLE_PROJECT: ${{ secrets.DBT_GOOGLE_PROJECT }}
          DBT_GOOGLE_KEYFILE: ${{ secrets.DBT_GOOGLE_KEYFILE }}

      # ───────────────────────────────────
      # 6. NETTOYAGE
      # ───────────────────────────────────
      - name: Drop CI schemas              # supprime les schemas temporaires de CI
        if: always()                       # même si le build a échoué
        run: uv run dbt run-operation drop_ci_schemas
        env:
          DBT_GOOGLE_PROJECT: ${{ secrets.DBT_GOOGLE_PROJECT }}
          DBT_GOOGLE_KEYFILE: ${{ secrets.DBT_GOOGLE_KEYFILE }}
```

### 14.4 Pipeline CD — Déploiement en preprod

```yaml
# .github/workflows/cd-preprod.yml
name: CD — Preprod

on:
  push:
    branches:
      - preprod                            # déclenché au merge sur preprod

env:
  DBT_PROFILES_DIR: .
  DBT_TARGET: preprod

jobs:
  deploy-preprod:
    name: Deploy to Preprod
    runs-on: ubuntu-latest

    steps:
      # 1. SETUP (identique à la CI)
      - name: Checkout code
        uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install uv
        uses: astral-sh/setup-uv@v4

      - name: Install dependencies
        run: uv sync --frozen

      - name: Install dbt packages
        run: uv run dbt deps

      # 2. BUILD COMPLET en preprod
      - name: Run dbt on preprod          # build complet (pas de Slim CI)
        run: uv run dbt build --target preprod
        env:
          DBT_GOOGLE_PROJECT: ${{ secrets.DBT_GOOGLE_PROJECT_PREPROD }}
          DBT_GOOGLE_KEYFILE: ${{ secrets.DBT_GOOGLE_KEYFILE_PREPROD }}

      # 3. DOCUMENTATION
      - name: Generate documentation       # génère la doc pour la preprod
        run: uv run dbt docs generate --target preprod
        env:
          DBT_GOOGLE_PROJECT: ${{ secrets.DBT_GOOGLE_PROJECT_PREPROD }}
          DBT_GOOGLE_KEYFILE: ${{ secrets.DBT_GOOGLE_KEYFILE_PREPROD }}

      # 4. SAUVEGARDE DU MANIFEST
      - name: Save preprod manifest        # sauvegarde pour la comparaison d'état
        run: |
          aws s3 cp target/manifest.json \
            s3://${{ secrets.ARTIFACT_BUCKET }}/preprod_manifest/manifest.json

      # 5. NOTIFICATION
      - name: Notify team — success
        if: success()
        uses: slackapi/slack-github-action@v1.26.0
        with:
          payload: '{"text": "✅ Preprod deployment successful — ${{ github.sha }}"}'
        env:
          SLACK_WEBHOOK_URL: ${{ secrets.SLACK_WEBHOOK_URL }}

      - name: Notify team — failure
        if: failure()
        uses: slackapi/slack-github-action@v1.26.0
        with:
          payload: '{"text": "🔴 Preprod deployment FAILED — ${{ github.sha }}"}'
        env:
          SLACK_WEBHOOK_URL: ${{ secrets.SLACK_WEBHOOK_URL }}
```

### 14.5 Approche progressive

Ne cherchez pas la perfection dès le début. Voici un chemin d'implémentation recommandé :

| Étape | Action | Impact |
|-------|--------|--------|
| 1 | `dbt compile` sur chaque PR | Attrape 80% des bugs, 20 min de setup |
| 2 | Ajouter Slim CI (`state:modified`) | Builds rapides et économiques |
| 3 | Ajouter SQLFluff et dbt-score | Qualité de code automatisée |
| 4 | CD automatisé (preprod → prod) | Déploiement fiable sans clic |
| 5 | Notifications Slack/Teams | Visibilité pour l'équipe |

---

## 15. Glossaire dbt

| Terme | Définition |
|-------|-----------|
| **Adapter** | Plugin qui connecte dbt à un data warehouse spécifique (BigQuery, Snowflake, Postgres, etc.). Chaque adapter traduit le SQL générique dbt en dialecte SQL du warehouse. |
| **Artifact** | Fichier JSON produit par dbt lors de l'exécution. Comprend `manifest.json` (état du projet), `run_results.json` (résultats d'exécution), `catalog.json` (métadonnées des tables). |
| **CTE** | Common Table Expression. Sous-requête nommée définie avec `WITH`. dbt recommande leur utilisation pour structurer les modèles (1 CTE = 1 étape de transformation). |
| **DAG** | Directed Acyclic Graph. Graphe orienté sans cycle représentant les dépendances entre les modèles. Construit automatiquement par dbt à partir des appels `ref()` et `source()`. |
| **Defer** | Mécanisme permettant de référencer les modèles d'un autre environnement (typiquement la production) quand ils n'ont pas été modifiés. Utilisé en Slim CI avec `--defer`. |
| **Ephemeral** | Matérialisation qui n'est pas persistée dans le warehouse. Le modèle est injecté comme une CTE dans les modèles qui le référencent. |
| **Exposure** | Déclaration d'une dépendance en aval du projet dbt (dashboard BI, API, notebook). Documente qui consomme les données et permet de tracer l'impact des changements. |
| **Full Refresh** | Exécution qui reconstruit un modèle incrémental de zéro (équivalent à un `DROP TABLE` + `CREATE TABLE`). Déclenché avec `--full-refresh`. |
| **Jinja** | Moteur de templating Python intégré à dbt. Permet d'ajouter de la logique (conditions, boucles, variables) dans les fichiers SQL. |
| **Lineage** | Traçabilité des données de bout en bout : de la source brute au dashboard final, en passant par chaque transformation dbt. Visible dans la documentation générée. |
| **Macro** | Fonction réutilisable écrite en Jinja. Peut être appelée dans les modèles, les tests et d'autres macros. Les packages dbt sont essentiellement des collections de macros. |
| **Manifest** | Fichier `manifest.json` contenant la représentation complète du projet dbt (modèles, tests, sources, macros, graphe). Utilisé pour la comparaison d'état (Slim CI). |
| **Model** | Fichier `.sql` contenant une instruction `SELECT`. dbt l'enveloppe dans un DDL approprié selon la matérialisation configurée. |
| **Node** | Tout objet dans le DAG dbt : model, test, source, seed, snapshot, exposure, metric. |
| **Package** | Module dbt réutilisable distribuable et installable via `packages.yml`. Contient des macros, des tests et potentiellement des modèles. |
| **Profile** | Configuration de connexion au data warehouse, définie dans `profiles.yml`. Contient les targets (dev, staging, prod). |
| **Seed** | Fichier CSV versionné dans Git et chargé dans le warehouse par `dbt seed`. Idéal pour les petites tables de référence. |
| **Selector** | Définition YAML nommée d'un groupe de nœuds. Permet de créer des sélections complexes réutilisables pour les commandes dbt. |
| **Slim CI** | Stratégie de CI qui ne build que les modèles modifiés (et leurs dépendants) en comparant le manifest actuel avec celui de production. |
| **Snapshot** | Mécanisme de capture des changements de données sources au fil du temps (SCD Type 2). Ajoute des colonnes d'historisation (`dbt_valid_from`, `dbt_valid_to`). |
| **Source** | Déclaration d'une table brute déjà présente dans le warehouse. Définie dans un fichier YAML et référencée avec `{{ source() }}`. |
| **State** | Comparaison entre le manifest actuel et un manifest de référence pour identifier les changements. Utilisé avec `--state` et `state:modified`. |
| **Target** | Environnement de destination défini dans `profiles.yml` (dev, staging, prod). Chaque target peut avoir sa propre base de données, son propre schema, etc. |
| **Test** | Assertion sur les données. Peut être générique (YAML : `not_null`, `unique`) ou singulier (fichier SQL custom). Un test passe si sa requête retourne 0 ligne. |

---

## 16. Erreurs courantes et debug

### 16.1 Erreur de connexion — `profiles.yml`

**Symptôme** :
```
Runtime Error: Could not find profile named 'mon_projet_dbt'
```

**Cause** : le champ `profile` dans `dbt_project.yml` ne correspond pas à un profil défini dans `profiles.yml`.

**Résolution** :
```yaml
# dbt_project.yml
profile: 'mon_projet_dbt'    # ← ce nom...

# profiles.yml
mon_projet_dbt:               # ← ...doit correspondre exactement à celui-ci
  target: dev
  outputs:
    dev:
      type: postgres
      # ...
```

Vérifier avec `dbt debug`.

---

### 16.2 Source introuvable

**Symptôme** :
```
Compilation Error: source 'ecommerce' was not found
```

**Cause** : la source n'est pas déclarée dans un fichier YAML, ou le fichier YAML n'est pas dans un dossier scanné par dbt.

**Résolution** :
1. Vérifier qu'un fichier `_sources.yml` (ou tout fichier `.yml`) dans `models/` contient la déclaration de la source.
2. Vérifier l'orthographe exacte du nom de la source.
3. S'assurer que le fichier est dans un chemin listé dans `model-paths` dans `dbt_project.yml`.

---

### 16.3 `ref()` incorrect

**Symptôme** :
```
Compilation Error: Model 'stg_orderss' was not found
```

**Cause** : faute de frappe dans le nom du modèle passé à `ref()`.

**Résolution** :
```sql
-- ❌ Erreur : le nom ne correspond à aucun fichier .sql
from {{ ref('stg_orderss') }}

-- ✅ Correct : correspond au fichier models/staging/stg_orders.sql
from {{ ref('stg_orders') }}
```

Le nom passé à `ref()` est le **nom du fichier sans l'extension `.sql`**.

---

### 16.4 Erreur SQL

**Symptôme** :
```
Database Error: Syntax error at or near "FORM"
```

**Cause** : erreur de syntaxe SQL dans le modèle.

**Résolution** :
1. Exécuter `dbt compile --select mon_modele` pour voir le SQL compilé.
2. Ouvrir `target/compiled/mon_projet/models/.../mon_modele.sql`.
3. Copier ce SQL et l'exécuter directement dans le warehouse pour identifier l'erreur.

---

### 16.5 Erreur Jinja

**Symptôme** :
```
Compilation Error: 'undefined' is undefined
```

**Cause** : une variable ou macro Jinja n'existe pas ou est mal orthographiée.

**Résolution** :
```sql
-- ❌ Variable non déclarée
where date >= '{{ var("star_date") }}'       -- typo dans le nom

-- ✅ Avec la bonne orthographe ET une valeur par défaut
where date >= '{{ var("start_date", "2024-01-01") }}'
```

---

### 16.6 Package manquant

**Symptôme** :
```
Compilation Error: Macro 'dbt_utils.generate_surrogate_key' not found
```

**Cause** : le package n'est pas installé ou n'est pas déclaré dans `packages.yml`.

**Résolution** :
```bash
# 1. Vérifier que le package est dans packages.yml
cat packages.yml

# 2. Installer/réinstaller les packages
dbt deps
```

---

### 16.7 Modèle incrémental mal configuré

**Symptôme** : le modèle incrémental duplique des données ou ne détecte pas les nouvelles lignes.

**Cause** : `unique_key` manquant ou `is_incremental()` mal utilisé.

**Résolution** :
```sql
-- ❌ Pas de unique_key → doublons possibles
{{ config(materialized='incremental') }}

-- ✅ Avec unique_key et filtre incrémental correct
{{
  config(
    materialized='incremental',
    unique_key='event_id',               -- clé pour la déduplication
    incremental_strategy='merge'         -- ou delete+insert
  )
}}

select ...
from {{ source('analytics', 'events') }}

{% if is_incremental() %}
    where event_timestamp > (select max(event_timestamp) from {{ this }})
{% endif %}
```

---

### 16.8 Problème de permissions

**Symptôme** :
```
Database Error: User does not have permission to create tables in schema 'production'
```

**Cause** : le rôle/utilisateur dbt n'a pas les droits nécessaires sur le schema cible.

**Résolution** :
1. Vérifier le rôle configuré dans `profiles.yml`.
2. Accorder les permissions nécessaires dans le warehouse :

```sql
-- Exemple Snowflake
GRANT USAGE ON DATABASE analytics TO ROLE transformer;
GRANT CREATE TABLE ON SCHEMA analytics.dbt_prod TO ROLE transformer;

-- Exemple PostgreSQL
GRANT CREATE ON SCHEMA dbt_prod TO dbt_user;
GRANT USAGE ON SCHEMA dbt_prod TO dbt_user;
```

---

## 17. Bonnes pratiques professionnelles

### 17.1 Conventions de nommage

| Couche | Préfixe | Exemple |
|--------|---------|---------|
| Staging | `stg_` | `stg_orders`, `stg_customers` |
| Intermediate | `int_` | `int_orders_enriched`, `int_daily_revenue` |
| Facts (marts) | `fct_` | `fct_revenue`, `fct_orders` |
| Dimensions (marts) | `dim_` | `dim_customers`, `dim_products` |
| Seeds | Pas de préfixe | `country_codes`, `payment_types` |
| Snapshots | `snapshot_` | `snapshot_customers` |

Pour les colonnes :
- **Clés primaires** : `<entité>_id` (ex : `order_id`, `customer_id`)
- **Booléens** : `is_` ou `has_` (ex : `is_active`, `has_subscription`)
- **Dates** : `<événement>_date` (ex : `order_date`, `created_date`)
- **Timestamps** : `<événement>_at` (ex : `created_at`, `updated_at`)
- **Montants** : avec l'unité (ex : `amount_euros`, `price_cents`)

### 17.2 Règle d'or : `ref()` et `source()`

```sql
-- ❌ JAMAIS de noms de tables en dur
select * from raw_database.public.orders

-- ✅ TOUJOURS utiliser source() pour les tables brutes
select * from {{ source('ecommerce', 'orders') }}

-- ✅ TOUJOURS utiliser ref() pour les modèles dbt
select * from {{ ref('stg_orders') }}
```

### 17.3 Structure SQL avec CTEs

```sql
-- ✅ Bonne pratique : une CTE par étape logique
with

source_data as (
    select * from {{ source('ecommerce', 'orders') }}
),

filtered as (
    select * from source_data where status != 'cancelled'
),

renamed as (
    select
        id as order_id,
        user_id as customer_id,
        amount / 100.0 as amount_euros
    from filtered
),

final as (
    select * from renamed
)

select * from final
```

### 17.4 Documentation et tests minimaux

Chaque modèle devrait avoir au minimum :
- Une **description** du modèle
- Des **descriptions** pour les colonnes clés
- Un test `unique` et `not_null` sur la clé primaire
- Un test `not_null` sur les champs critiques

### 17.5 Organisation des fichiers YAML

```
models/
  staging/
    _stg_sources.yml       # déclarations de sources
    _stg_schema.yml        # documentation et tests des modèles staging
    stg_orders.sql
    stg_customers.sql
  marts/
    _marts_schema.yml      # documentation et tests des marts
    fct_revenue.sql
    dim_customers.sql
```

Convention : les fichiers YAML commencent par `_` pour apparaître en premier dans l'explorateur de fichiers.

### 17.6 Performance et coûts

1. **Commencer par des views**, passer en table/incremental seulement quand c'est nécessaire.
2. **Éviter d'empiler trop de views** — si la performance se dégrade, matérialiser les couches intermédiaires.
3. **Utiliser des modèles incrémentaux** pour les tables à gros volume (logs, événements).
4. **Limiter les colonnes** avec `select` explicite au lieu de `select *` dans les marts.
5. **Utiliser le Slim CI** pour réduire les coûts des builds en CI.

---

## 18. Template de projet dbt réutilisable

### 18.1 Structure du repository

```
mon_projet_dbt/
├── .github/
│   └── workflows/
│       ├── ci.yml                    # Pipeline CI
│       └── cd-preprod.yml            # Pipeline CD preprod
│
├── models/
│   ├── staging/
│   │   ├── _stg_sources.yml          # Sources déclarées
│   │   ├── _stg_schema.yml           # Tests et docs staging
│   │   └── stg_<entity>.sql          # Un modèle par source
│   │
│   ├── intermediate/
│   │   ├── _int_schema.yml           # Tests et docs intermediate
│   │   └── int_<description>.sql     # Logique métier intermédiaire
│   │
│   └── marts/
│       ├── <domain>/                 # Organisé par domaine métier
│       │   ├── _<domain>_schema.yml
│       │   ├── fct_<entity>.sql      # Tables de faits
│       │   └── dim_<entity>.sql      # Tables de dimensions
│       └── ...
│
├── macros/
│   ├── generate_schema_name.sql
│   └── limit_for_ci.sql
│
├── tests/
│   └── assert_<description>.sql
│
├── seeds/
│   └── <reference_data>.csv
│
├── snapshots/
│   └── snapshot_<entity>.sql
│
├── analyses/
│   └── <ad_hoc_analysis>.sql
│
├── dbt_project.yml
├── packages.yml
├── profiles.yml                      # (ou dans ~/.dbt/)
├── selectors.yml
├── .sqlfluff
├── .yamllint.yml
└── .gitignore
```

### 18.2 Template `dbt_project.yml`

```yaml
# dbt_project.yml — Configuration principale du projet
name: 'mon_projet_dbt'                  # nom unique du projet (snake_case)
version: '1.0.0'                        # version sémantique du projet
config-version: 2                       # version du format de configuration

profile: 'mon_projet_dbt'              # doit correspondre à profiles.yml

# Chemins des dossiers
model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

# Dossiers nettoyés par `dbt clean`
clean-targets:
  - "target"
  - "dbt_packages"

# Configuration des modèles
models:
  mon_projet_dbt:
    staging:
      +materialized: view              # staging toujours en view
      +tags: ['staging']
    intermediate:
      +materialized: view              # intermediate en view par défaut
    marts:
      +materialized: table             # marts en table pour la BI
      +tags: ['marts']

# Variables globales
vars:
  start_date: '2024-01-01'
  is_ci: false                          # surchargé en CI avec --vars
```

### 18.3 Template `schema.yml`

```yaml
# models/staging/_stg_schema.yml
version: 2

models:
  - name: stg_orders
    description: "Commandes nettoyées depuis la source e-commerce"
    columns:
      - name: order_id
        description: "Identifiant unique de la commande"
        tests:
          - unique
          - not_null
      - name: customer_id
        description: "Identifiant du client"
        tests:
          - not_null
      - name: amount_euros
        description: "Montant de la commande en euros"
        tests:
          - not_null
```

### 18.4 Checklists

#### Checklist démarrage de projet

- [ ] `dbt init` — Initialiser le projet
- [ ] Configurer `profiles.yml` avec les informations de connexion
- [ ] `dbt debug` — Vérifier la connexion
- [ ] Configurer `dbt_project.yml` (nom, paths, matérialisations)
- [ ] Créer les fichiers `_sources.yml` pour les tables sources
- [ ] Créer les premiers modèles staging (`stg_`)
- [ ] Ajouter `packages.yml` avec `dbt_utils` et `dbt_expectations`
- [ ] `dbt deps` — Installer les packages
- [ ] `dbt run` — Premier run
- [ ] `dbt test` — Premiers tests
- [ ] Créer le `.gitignore` (exclure `target/`, `dbt_packages/`, `logs/`)

#### Checklist qualité

- [ ] Chaque modèle a une description
- [ ] Les colonnes clés sont documentées
- [ ] La clé primaire a des tests `unique` + `not_null`
- [ ] Les champs critiques ont des tests `not_null`
- [ ] Les `ref()` et `source()` sont utilisés (pas de noms en dur)
- [ ] Les modèles utilisent des CTEs nommées
- [ ] Le SQL est linté (SQLFluff)
- [ ] Le YAML est linté (yamllint)
- [ ] Les modèles suivent les conventions de nommage (`stg_`, `fct_`, `dim_`)

#### Checklist mise en production

- [ ] Tous les tests passent (`dbt test`)
- [ ] La compilation est OK (`dbt compile`)
- [ ] Le linting SQL est clean (`sqlfluff lint`)
- [ ] La documentation est à jour (`dbt docs generate`)
- [ ] Le CI/CD est en place
- [ ] Les credentials sont dans des secrets (GitHub Secrets, env vars)
- [ ] Le `profiles.yml` n'est PAS dans le repo Git
- [ ] Les matérialisations sont appropriées (table pour les marts, incremental pour les gros volumes)
- [ ] Le manifest de production est sauvegardé (pour le Slim CI)
- [ ] Les notifications d'équipe sont configurées (Slack, Teams)

---

## 19. Plan d'apprentissage dbt

### Niveau 1 — Débutant (1 à 2 semaines)

| Jour | Thème | Actions |
|------|-------|---------|
| 1-2 | Installation | Installer dbt, créer un premier projet, configurer `profiles.yml`, `dbt debug` |
| 3-4 | Premiers modèles | Comprendre `source()` et `ref()`, écrire 2-3 modèles staging |
| 5-6 | Tests basiques | Tests natifs (`unique`, `not_null`, `accepted_values`), schema.yml |
| 7-8 | Matérialisations | View vs Table, configurer via `dbt_project.yml` et `config()` |
| 9-10 | Documentation | `dbt docs generate`, `dbt docs serve`, documenter ses modèles |

**Objectif** : être capable de créer un pipeline staging → marts simple, avec des tests et de la documentation.

### Niveau 2 — Intermédiaire (2 à 4 semaines)

| Semaine | Thème | Actions |
|---------|-------|---------|
| 1 | Jinja et macros | Conditions, boucles, créer ses premières macros, variables |
| 2 | Packages | Installer `dbt_utils`, `dbt_expectations`, utiliser les tests avancés |
| 3 | Incremental | Modèles incrémentaux (merge, `is_incremental()`), seeds, snapshots |
| 4 | Architecture | Organisation staging/intermediate/marts, conventions de nommage, sélecteurs |

**Objectif** : maîtriser les modèles incrémentaux, créer des macros, organiser un projet scalable.

### Niveau 3 — Avancé (4+ semaines)

| Semaine | Thème | Actions |
|---------|-------|---------|
| 1-2 | CI/CD | Pipeline GitHub Actions, Slim CI, échantillonnage, SQLFluff |
| 3 | Stratégies incrémentales | Merge vs delete+insert vs microbatch, partitioning |
| 4 | Architecture avancée | Multi-environnements, custom materializations, dbt mesh |
| 5+ | Optimisation | Performance du warehouse, coûts, dbt-score, monitoring |

**Objectif** : mettre en place un CI/CD complet, optimiser les performances, gérer un projet dbt en production à grande échelle.

### Ressources recommandées

| Ressource | Type | Lien |
|-----------|------|------|
| dbt Documentation officielle | Documentation | [docs.getdbt.com](https://docs.getdbt.com) |
| dbt Learn (gratuit) | Cours en ligne | [learn.getdbt.com](https://learn.getdbt.com) |
| dbt Community Slack | Communauté | [community.getdbt.com](https://community.getdbt.com) |
| dbt Hub (packages) | Registry | [hub.getdbt.com](https://hub.getdbt.com) |
| dbt Best Practices | Guide | [docs.getdbt.com/best-practices](https://docs.getdbt.com/best-practices) |

---

<p align="center">
  <em>Ce document est un support vivant. Contribuez, corrigez, améliorez-le.</em>
</p>

<p align="center">
  <strong>Licence</strong> : MIT — Utilisez-le librement pour vos projets et formations.
</p>


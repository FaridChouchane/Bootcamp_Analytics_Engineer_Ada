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

### 2.2 Explication détaillée de chaque élément

---

#### `models/` — Le cœur du projet

Le dossier `models/` contient l'ensemble de vos transformations SQL. Chaque fichier `.sql` est un **modèle**. Un modèle est fondamentalement une instruction `SELECT` — rien de plus. C'est dbt qui se charge d'envelopper ce `SELECT` dans le DDL approprié (`CREATE VIEW AS ...`, `CREATE TABLE AS ...`, etc.) selon la matérialisation que vous avez configurée.

**Mécanisme interne** : quand vous lancez `dbt run`, voici ce qui se passe pour chaque modèle :

1. dbt lit le fichier `.sql`
2. Il résout tout le Jinja (les `{{ ref() }}`, `{{ source() }}`, les conditions `{% if %}`, etc.)
3. Il obtient du SQL pur
4. Il enveloppe ce SQL dans un DDL selon la matérialisation (`CREATE VIEW AS select ...` ou `CREATE TABLE AS select ...`)
5. Il envoie ce DDL au warehouse pour exécution
6. Le warehouse crée l'objet (vue, table, etc.)

**Ordre d'exécution** : dbt ne lance pas les modèles dans l'ordre alphabétique. Il analyse tous les appels `ref()` pour construire un **DAG** (Directed Acyclic Graph) — un graphe de dépendances — puis exécute les modèles dans l'ordre topologique. Si `fct_revenue` appelle `{{ ref('stg_orders') }}`, dbt garantit que `stg_orders` sera exécuté avant `fct_revenue`.

**La convention en 3 couches** recommandée par dbt Labs organise les modèles de manière à séparer les responsabilités :

##### Couche 1 : `staging/` — Le nettoyage des sources

```
models/
  staging/
    _stg_sources.yml          # déclaration des sources
    _stg_schema.yml           # documentation et tests
    stg_orders.sql            # 1 modèle = 1 source table
    stg_customers.sql
    stg_products.sql
```

**Rôle** : chaque modèle staging correspond à **exactement une table source**. C'est une couche de nettoyage et de normalisation. Le staging est la seule couche qui touche directement aux sources — toutes les couches suivantes travaillent sur des modèles staging via `ref()`.

**Ce que fait un modèle staging** :
- Renomme les colonnes (ex : `usr_id` → `customer_id`) pour avoir des noms lisibles et cohérents
- Corrige les types de données (ex : `cast(amount as decimal(10,2))`)
- Filtre les données invalides évidentes (ex : montants négatifs, dates dans le futur)
- Sélectionne explicitement les colonnes utiles (pas de `select *` en sortie finale)
- Ne fait **aucune** jointure et **aucun** calcul métier — c'est le rôle des couches suivantes

**Conventions** :
- Préfixe : `stg_`
- Un modèle par table source
- Matérialisation : `view` (toujours à jour, pas de coût de stockage)
- Jamais de jointure entre sources dans cette couche

```sql
-- models/staging/stg_orders.sql
-- Exemple type d'un modèle staging

{{
  config(
    materialized='view'                -- view car c'est du staging
  )
}}

with source as (

    -- Lecture de la source brute (la seule couche qui utilise source())
    select * from {{ source('ecommerce', 'orders') }}

),

renamed as (

    select
        -- Renommage explicite de chaque colonne
        id              as order_id,
        usr_id          as customer_id,
        ord_date        as order_date,
        sts             as order_status,
        amt             as amount_cents,       -- on garde les centimes ici
        pay_type        as payment_type_code,
        created         as created_at,
        updated         as updated_at
    from source

),

filtered as (

    select *
    from renamed
    where
        order_id is not null            -- exclure les lignes sans ID
        and amount_cents >= 0           -- exclure les montants aberrants
        and order_date >= '2020-01-01'  -- exclure les dates trop anciennes

)

select * from filtered
```

##### Couche 2 : `intermediate/` — La logique métier intermédiaire

```
models/
  intermediate/
    _int_schema.yml
    int_orders_enriched.sql       # jointure commandes + clients
    int_daily_revenue.sql         # agrégation journalière
    int_customer_orders.sql       # métriques par client
```

**Rôle** : les modèles intermédiaires combinent, enrichissent et transforment les données provenant du staging. Ils contiennent la logique métier qui serait trop complexe pour un seul modèle mart mais qui n'a pas vocation à être exposée directement à la BI.

**Ce que fait un modèle intermediate** :
- Jointures entre modèles staging (ex : commandes + clients + produits)
- Calculs métier complexes (ex : fenêtres d'analyse, scoring, classification)
- Agrégations intermédiaires
- Dénormalisation de données pour préparer les marts

**Conventions** :
- Préfixe : `int_`
- Matérialisation : `view` ou `ephemeral` (pas besoin de les persister en général)
- Les utilisateurs BI ne requêtent jamais directement cette couche
- Un modèle intermédiaire peut référencer des modèles staging ET d'autres intermédiaires

```sql
-- models/intermediate/int_orders_enriched.sql
-- Jointure entre commandes et clients pour enrichir les données

with orders as (

    select * from {{ ref('stg_orders') }}         -- source : staging

),

customers as (

    select * from {{ ref('stg_customers') }}      -- source : staging

),

products as (

    select * from {{ ref('stg_products') }}       -- source : staging

),

enriched as (

    select
        o.order_id,
        o.order_date,
        o.amount_cents,

        -- Enrichissement depuis la table clients
        c.customer_name,
        c.customer_segment,
        c.signup_date,

        -- Enrichissement depuis la table produits
        p.product_name,
        p.product_category,

        -- Calcul métier : rang de la commande pour chaque client
        row_number() over (
            partition by o.customer_id
            order by o.order_date
        ) as order_sequence_number,

        -- Calcul métier : est-ce la première commande ?
        case
            when row_number() over (
                partition by o.customer_id
                order by o.order_date
            ) = 1 then true
            else false
        end as is_first_order

    from orders o
    left join customers c on o.customer_id = c.customer_id
    left join products p on o.product_id = p.product_id

)

select * from enriched
```

##### Couche 3 : `marts/` — Les modèles prêts pour la BI

```
models/
  marts/
    finance/                     # domaine métier : finance
      _finance_schema.yml
      fct_revenue.sql            # table de faits : revenu
      fct_invoices.sql           # table de faits : factures
      dim_customers.sql          # table de dimension : clients
    marketing/                   # domaine métier : marketing
      _marketing_schema.yml
      fct_campaigns.sql
      fct_conversions.sql
    product/                     # domaine métier : produit
      _product_schema.yml
      fct_usage_events.sql
      dim_features.sql
```

**Rôle** : les marts sont les modèles finaux, exposés aux utilisateurs BI et aux outils de visualisation (Looker, Metabase, Power BI, Tableau). Ils doivent être optimisés pour la lecture et organisés par **domaine métier**, pas par source de données.

**Ce que fait un modèle mart** :
- Agrégation finale des données
- Structure adaptée aux requêtes BI (schéma en étoile : faits + dimensions)
- Documentation exhaustive (chaque colonne doit être documentée car les utilisateurs finaux les consultent)

**Conventions** :
- Préfixes : `fct_` pour les tables de faits, `dim_` pour les tables de dimensions
- Matérialisation : `table` ou `incremental` (les outils BI requêtent directement ces tables — elles doivent être rapides)
- Organisés par domaine métier (finance, marketing, produit) et non par source
- Documentation et tests exhaustifs car c'est la couche visible

**Tables de faits vs dimensions** :
- **Faits** (`fct_`) : événements, transactions, mesures. Contiennent des métriques numériques et des clés étrangères vers les dimensions. Ex : `fct_revenue`, `fct_orders`, `fct_page_views`.
- **Dimensions** (`dim_`) : entités de référence. Contiennent les attributs descriptifs. Ex : `dim_customers`, `dim_products`, `dim_dates`.

```sql
-- models/marts/finance/fct_revenue.sql
-- Table de faits : revenu par commande, prête pour la BI

{{
  config(
    materialized='table',            -- table car requêté directement par la BI
    tags=['finance', 'daily']
  )
}}

with orders as (

    select * from {{ ref('int_orders_enriched') }}    -- source : intermediate

),

final as (

    select
        -- Clés
        order_id,
        customer_id,

        -- Dimensions (clés étrangères)
        customer_segment,
        product_category,

        -- Métriques / faits
        amount_cents / 100.0 as amount_euros,
        is_first_order,
        order_sequence_number,

        -- Temporalité
        order_date,
        date_trunc('month', order_date) as order_month,
        date_trunc('quarter', order_date) as order_quarter

    from orders
    where order_status = 'completed'       -- uniquement les commandes finalisées

)

select * from final
```

**Pourquoi cette organisation en 3 couches ?**

| Avantage | Explication |
|----------|------------|
| **Lisibilité** | Chaque modèle a une responsabilité claire et limitée |
| **Maintenabilité** | Un changement de source n'impacte que le staging, pas les marts |
| **Testabilité** | On peut tester chaque couche indépendamment |
| **Performance** | On peut matérialiser différemment chaque couche (view staging, table marts) |
| **Collaboration** | Un analytics engineer peut travailler sur un mart sans toucher au staging |
| **Réutilisabilité** | Un même modèle staging peut alimenter plusieurs marts de domaines différents |

**Le flux de données** :

```
Sources brutes (warehouse)
        │
        ▼
   ┌─────────┐
   │ staging  │  ← source()  ← 1:1 avec les tables sources
   │ stg_*    │  ← renommage, typage, filtrage basique
   └────┬─────┘
        │ ref()
        ▼
   ┌──────────────┐
   │ intermediate │  ← jointures, enrichissement, calculs
   │ int_*        │  ← logique métier intermédiaire
   └────┬─────────┘
        │ ref()
        ▼
   ┌─────────┐
   │  marts  │  ← agrégations finales, schéma en étoile
   │ fct_*   │  ← optimisé pour la BI
   │ dim_*   │  ← documenté pour les utilisateurs finaux
   └─────────┘
        │
        ▼
   Outils BI (Looker, Metabase, Power BI...)
```

---

#### `macros/` — Fonctions réutilisables

```
macros/
  generate_schema_name.sql     # surcharge de la macro native dbt
  cents_to_euros.sql           # conversion de devises
  safe_divide.sql              # division sécurisée (évite le /0)
  limit_for_ci.sql             # limitation des données en CI
  drop_ci_schemas.sql          # nettoyage des schemas de CI
```

**Rôle** : les macros sont des **fonctions Jinja réutilisables**. Elles permettent de factoriser de la logique SQL qu'on retrouverait en doublon dans plusieurs modèles. Pensez-y comme des fonctions dans un langage de programmation classique.

**Mécanisme interne** : une macro est définie dans un fichier `.sql` du dossier `macros/` avec la syntaxe `{% macro nom(params) %}...{% endmacro %}`. Elle est ensuite appelable depuis n'importe quel modèle, test ou autre macro avec `{{ nom(params) }}`. Au moment de la compilation, dbt remplace l'appel par le contenu de la macro.

**Trois catégories de macros** :

1. **Macros utilitaires** : logique SQL réutilisable dans vos modèles.

```sql
-- macros/safe_divide.sql
{% macro safe_divide(numerator, denominator, default_value=0) %}
    case
        when {{ denominator }} is null or {{ denominator }} = 0
        then {{ default_value }}
        else cast({{ numerator }} as decimal) / nullif({{ denominator }}, 0)
    end
{% endmacro %}

-- Appel dans un modèle :
-- select {{ safe_divide('revenue', 'order_count') }} as avg_order_value
-- Compilé en →
-- select case when order_count is null or order_count = 0
--        then 0 else cast(revenue as decimal) / nullif(order_count, 0) end
--        as avg_order_value
```

2. **Macros de surcharge** : remplacent le comportement natif de dbt. La plus courante est `generate_schema_name` qui contrôle comment dbt nomme les schemas dans le warehouse.

```sql
-- macros/generate_schema_name.sql
-- Si vous ne surchargez pas cette macro, dbt ajoute un préfixe au schema
-- Ex : vous configurez +schema: finance → dbt crée "dbt_dev_finance" au lieu de "finance"
-- Cette surcharge permet d'avoir "finance" directement en prod

{% macro generate_schema_name(custom_schema_name, node) %}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}            -- en prod : "finance"
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}  -- en dev : "dbt_jean_finance"
    {%- endif -%}
{% endmacro %}
```

3. **Macros d'administration** : exécutées directement via `dbt run-operation`, pas depuis un modèle. Utilisées pour des tâches de maintenance (nettoyage, permissions, etc.).

```sql
-- macros/drop_ci_schemas.sql
-- Appelée par : dbt run-operation drop_ci_schemas

{% macro drop_ci_schemas() %}
    {% set query %}
        select schema_name from information_schema.schemata
        where schema_name like 'dbt_ci_%'
    {% endset %}
    {% if execute %}
        {% for row in run_query(query) %}
            {% do run_query("drop schema if exists " ~ row['schema_name'] ~ " cascade") %}
        {% endfor %}
    {% endif %}
{% endmacro %}
```

**Macros intégrées à dbt** (disponibles sans rien installer) :
- `{{ ref('model_name') }}` — référence un modèle
- `{{ source('source', 'table') }}` — référence une source
- `{{ var('name', default) }}` — accède à une variable
- `{{ env_var('ENV_VAR') }}` — lit une variable d'environnement
- `{{ is_incremental() }}` — true si le modèle tourne en mode incrémental
- `{{ this }}` — référence la table actuelle du modèle (pour les incrémentaux)
- `{{ log('message', info=true) }}` — écrit dans les logs
- `{{ run_query('sql') }}` — exécute du SQL pendant la compilation

**Macros de packages** (installées via `packages.yml`) :
- `{{ dbt_utils.generate_surrogate_key(['col1', 'col2']) }}` — clé de substitution
- `{{ dbt_utils.pivot('column', values, ...) }}` — pivoter des lignes en colonnes
- `{{ dbt_utils.date_spine(datepart, start_date, end_date) }}` — générer une série de dates

---

#### `tests/` — Tests singuliers (SQL custom)

```
tests/
  assert_positive_revenue.sql
  assert_no_orphan_orders.sql
  assert_twelve_months_coverage.sql
```

**Rôle** : le dossier `tests/` contient des **tests singuliers** — des requêtes SQL custom que vous écrivez pour vérifier des assertions spécifiques sur vos données. Ils complètent les tests génériques (déclarés dans les fichiers YAML) pour couvrir des cas métier plus complexes.

**Mécanisme interne** : un test singulier est un fichier `.sql` qui contient un `SELECT`. dbt exécute cette requête et **compte les lignes retournées**. Si la requête retourne **0 ligne**, le test passe. Si elle retourne **1 ou plusieurs lignes**, le test échoue — chaque ligne retournée représente un cas en erreur.

Pensez-y comme une requête qui cherche les problèmes : si elle ne trouve rien, tout va bien.

```sql
-- tests/assert_positive_revenue.sql
-- Ce test cherche les clients avec un revenu négatif
-- S'il en trouve → le test ÉCHOUE
-- S'il n'en trouve pas → le test PASSE

select
    customer_id,                    -- identifiant du client fautif
    lifetime_revenue                -- montant négatif trouvé
from {{ ref('fct_revenue') }}
where lifetime_revenue < 0          -- condition de recherche des erreurs
-- Si cette requête retourne 0 ligne → ✅ PASS
-- Si cette requête retourne 3 lignes → ❌ FAIL (3 clients avec revenu négatif)
```

```sql
-- tests/assert_no_orphan_orders.sql
-- Ce test vérifie l'intégrité référentielle :
-- chaque commande doit être associée à un client existant

select
    o.order_id,
    o.customer_id
from {{ ref('stg_orders') }} o
left join {{ ref('stg_customers') }} c
    on o.customer_id = c.customer_id
where c.customer_id is null          -- commandes dont le client n'existe pas
-- 0 ligne → tous les clients existent → ✅
-- N lignes → N commandes orphelines → ❌
```

**Différence avec les tests génériques** (YAML) :

| Aspect | Tests génériques (YAML) | Tests singuliers (SQL) |
|--------|------------------------|----------------------|
| Emplacement | `_schema.yml` dans `models/` | `tests/*.sql` |
| Syntaxe | Déclarative (YAML) | Impérative (SQL) |
| Réutilisabilité | Un test = plusieurs colonnes | Un test = un cas spécifique |
| Complexité | Simple (not_null, unique) | Illimitée (jointures, agrégations) |
| Exemple | `- unique` | Vérifier que les 12 mois sont couverts |

**Quand utiliser un test singulier** :
- Vérification métier complexe (ex : le total des lignes de commande = le total de la commande)
- Assertions cross-tables (ex : pas de commandes orphelines)
- Vérifications d'agrégats (ex : le revenu total est cohérent avec le mois précédent)
- Tests qui nécessitent du SQL complexe (fenêtres, sous-requêtes)

---

#### `seeds/` — Données de référence

```
seeds/
  country_codes.csv
  payment_types.csv
  currency_rates.csv
```

**Rôle** : les seeds sont des **fichiers CSV versionnés dans Git** qui sont chargés dans le data warehouse comme des tables. Ils servent à stocker des données de référence qui changent rarement et que vous voulez contrôler par le code plutôt que par un système d'ingestion.

**Mécanisme interne** : quand vous lancez `dbt seed`, dbt lit chaque fichier CSV du dossier `seeds/`, crée une table dans le warehouse (ou la remplace si elle existe), et y insère les données du CSV ligne par ligne. La table résultante est ensuite utilisable via `{{ ref('nom_du_seed') }}` exactement comme un modèle.

```csv
# seeds/payment_types.csv
code,label,is_electronic
1,Credit card,true
2,Cash,false
3,Bank transfer,true
4,Voucher,false
5,No charge,false
```

```sql
-- Utilisation dans un modèle staging :
-- Le seed est référencé via ref() comme n'importe quel modèle

select
    o.order_id,
    o.payment_type_code,
    pt.label as payment_method,           -- enrichissement depuis le seed
    pt.is_electronic
from {{ ref('stg_orders') }} o
left join {{ ref('payment_types') }} pt   -- jointure avec le seed
    on o.payment_type_code = pt.code
```

**Cas d'usage** :
- Tables de mapping (codes pays, codes devises, codes de paiement)
- Tables de correspondance (ancien ID → nouvel ID après une migration)
- Tables de paramétrage (taux de TVA par pays, seuils d'alerte)
- Données de test (jeux de données fixes pour les tests unitaires)
- Listes de référence (catégories, statuts, types)

**Ce qu'il ne faut PAS mettre dans les seeds** :
- Données volumineuses (> quelques milliers de lignes) → utilisez l'ingestion classique
- Données qui changent souvent → utilisez une source
- Données sensibles (mots de passe, tokens) → utilisez des variables d'environnement
- Résultats de requêtes → c'est le rôle des modèles

**Configuration dans dbt_project.yml** :

```yaml
seeds:
  mon_projet:
    +schema: reference                      # schema dédié aux seeds
    country_codes:                          # configuration par seed
      +column_types:                        # forcer les types de colonnes
        code: varchar(3)
        population: bigint
```

---

#### `snapshots/` — Historisation (SCD Type 2)

```
snapshots/
  snapshot_customers.sql
  snapshot_products.sql
  snapshot_pricing.sql
```

**Rôle** : les snapshots capturent **l'historique des changements** d'une table source au fil du temps. Si un client change de segment (de "Silver" à "Gold"), un modèle classique ne verrait que l'état actuel ("Gold"). Un snapshot conserve les deux états avec des dates de validité.

**Mécanisme interne** : quand vous lancez `dbt snapshot`, voici ce que dbt fait pour chaque snapshot :

1. Il lit la table source
2. Il compare chaque ligne avec ce qu'il a déjà stocké (identifié par la `unique_key`)
3. **Nouvelle ligne** (pas dans le snapshot) → elle est insérée avec `dbt_valid_from = now()` et `dbt_valid_to = null`
4. **Ligne modifiée** (elle existe mais des colonnes ont changé) → l'ancienne version reçoit `dbt_valid_to = now()`, la nouvelle version est insérée avec `dbt_valid_from = now()` et `dbt_valid_to = null`
5. **Ligne inchangée** → rien ne se passe
6. **Ligne supprimée** (optionnel) → l'ancienne version reçoit `dbt_valid_to = now()`

Le résultat est une table qui contient **toutes les versions** de chaque ligne, avec des plages temporelles de validité.

```sql
-- snapshots/snapshot_customers.sql

{% snapshot snapshot_customers %}

{{
  config(
    target_schema='snapshots',         -- schema dédié aux snapshots
    unique_key='customer_id',          -- clé pour identifier chaque entité
    strategy='timestamp',              -- stratégie : comparer via un timestamp
    updated_at='updated_at',           -- colonne de dernière modification
    invalidate_hard_deletes=true       -- marquer les lignes supprimées de la source
  )
}}

select
    customer_id,
    customer_name,
    customer_email,
    customer_segment,                   -- ce champ peut changer avec le temps
    subscription_plan,                  -- ce champ aussi
    updated_at
from {{ source('ecommerce', 'customers') }}

{% endsnapshot %}
```

**Résultat dans le warehouse** (exemple) :

| customer_id | customer_name | customer_segment | dbt_valid_from | dbt_valid_to | dbt_scd_id |
|-------------|--------------|-----------------|----------------|--------------|------------|
| 42 | Alice Martin | Silver | 2024-01-15 | 2024-06-01 | abc123 |
| 42 | Alice Martin | Gold | 2024-06-01 | null | def456 |
| 42 | Alice Martin | Gold | 2024-06-01 | null | def456 |

La ligne avec `dbt_valid_to = null` est la **version actuelle**. Les autres sont l'historique.

**Deux stratégies de détection des changements** :

| Stratégie | Quand l'utiliser | Comment ça marche |
|-----------|-----------------|-------------------|
| `timestamp` | Quand la source a une colonne `updated_at` fiable | Compare le timestamp — si plus récent, c'est un changement |
| `check` | Quand il n'y a pas de timestamp fiable | Compare les valeurs de colonnes spécifiées — si différentes, c'est un changement |

```sql
-- Stratégie 'check' — quand il n'y a pas de colonne updated_at
{% snapshot snapshot_pricing %}
{{
  config(
    target_schema='snapshots',
    unique_key='product_id',
    strategy='check',                       -- compare les colonnes listées ci-dessous
    check_cols=['price', 'discount_pct']    -- si l'un de ces champs change → nouvelle version
  )
}}
select product_id, price, discount_pct, product_name
from {{ source('catalog', 'products') }}
{% endsnapshot %}
```

**Cas d'usage concrets** :
- Historique des prix produits (pour analyser l'impact des changements de prix)
- Historique des segments clients (pour mesurer les montées/descentes en gamme)
- Historique des statuts (pour calculer des durées dans chaque statut)
- Conformité réglementaire (certains secteurs exigent de conserver l'historique)

---

#### `analyses/` — Requêtes non matérialisées

```
analyses/
  monthly_revenue_check.sql
  customer_cohort_exploration.sql
  data_quality_audit.sql
```

**Rôle** : le dossier `analyses/` contient des requêtes SQL qui **bénéficient du moteur Jinja de dbt** (vous pouvez utiliser `{{ ref() }}`, `{{ source() }}`, des macros, des variables) mais qui ne sont **jamais exécutées** automatiquement et ne créent **aucun objet** dans le warehouse.

**Mécanisme interne** : quand vous lancez `dbt compile`, dbt compile les analyses (résolution du Jinja → SQL pur) et place le résultat dans `target/compiled/mon_projet/analyses/`. Vous pouvez ensuite copier ce SQL compilé et l'exécuter manuellement dans votre outil SQL préféré.

`dbt run` et `dbt build` **ignorent complètement** les analyses. Elles ne sont jamais exécutées automatiquement.

```sql
-- analyses/monthly_revenue_check.sql
-- Requête d'audit mensuel — compilée par dbt mais exécutée manuellement

select
    date_trunc('month', order_date) as month,
    count(distinct customer_id) as unique_customers,
    count(*) as total_orders,
    sum(amount_euros) as total_revenue,
    sum(amount_euros) / count(*) as avg_order_value
from {{ ref('fct_revenue') }}                         -- bénéficie de ref()
where order_date >= '{{ var("start_date") }}'         -- bénéficie de var()
group by date_trunc('month', order_date)
order by month desc
```

**Cas d'usage** :
- Requêtes d'exploration ad hoc qu'on veut versionner dans Git
- Audits de données ponctuels
- Prototypage de futurs modèles (tester une logique avant d'en faire un modèle)
- Requêtes de support/debug que l'équipe partage

**Différence avec un modèle** :
- Un modèle crée un objet dans le warehouse (vue, table)
- Une analyse ne crée rien — elle est juste compilée

---

#### `dbt_project.yml` — Configuration du projet

**Rôle** : c'est le fichier qui **définit un dossier comme un projet dbt**. Sans ce fichier, dbt ne reconnaît pas le dossier comme un projet. Il contient la configuration globale : nom du projet, chemins des dossiers, matérialisations par défaut, variables, et bien plus.

**Mécanisme de précédence** : dbt applique les configurations du plus global au plus spécifique. Si vous définissez une matérialisation dans `dbt_project.yml` ET dans un bloc `{{ config() }}` dans le modèle, le bloc `config()` **prend le dessus**.

```
Ordre de précédence (du plus faible au plus fort) :
1. dbt_project.yml    ← configuration globale (la plus faible)
2. schema.yml         ← configuration par modèle dans le YAML
3. {{ config() }}     ← configuration dans le fichier SQL du modèle (la plus forte)
```

```yaml
# dbt_project.yml — Exemple complet commenté

# ═══════════════════════════════════════════════════════
# IDENTIFICATION DU PROJET
# ═══════════════════════════════════════════════════════
name: 'mon_projet_dbt'            # nom unique du projet (snake_case, obligatoire)
                                   # utilisé comme namespace pour les macros et configs
config-version: 2                  # version du format de config (toujours 2)
version: '1.0.0'                   # version sémantique de votre projet

# Profil de connexion (doit correspondre à une entrée dans profiles.yml)
profile: 'mon_projet_dbt'

# ═══════════════════════════════════════════════════════
# CHEMINS DES DOSSIERS
# ═══════════════════════════════════════════════════════
# Les valeurs ci-dessous sont les défauts — déclarez-les seulement si vous changez
model-paths: ["models"]            # où trouver les modèles SQL
analysis-paths: ["analyses"]       # où trouver les analyses
test-paths: ["tests"]              # où trouver les tests singuliers
seed-paths: ["seeds"]              # où trouver les fichiers CSV
macro-paths: ["macros"]            # où trouver les macros Jinja
snapshot-paths: ["snapshots"]      # où trouver les snapshots
# Note : les chemins sont des listes — vous pouvez avoir plusieurs dossiers :
# model-paths: ["models", "models_legacy"]

# Dossiers à supprimer lors d'un `dbt clean`
clean-targets:
  - "target"                       # dossier de compilation
  - "dbt_packages"                 # packages installés

# ═══════════════════════════════════════════════════════
# CONFIGURATION DES MODÈLES (matérialisations par défaut)
# ═══════════════════════════════════════════════════════
models:
  mon_projet_dbt:                  # doit correspondre au 'name' ci-dessus

    staging:                       # s'applique à models/staging/**
      +materialized: view          # tous les modèles staging en view
      +tags: ['staging']           # tag pour la sélection groupée

    intermediate:                  # s'applique à models/intermediate/**
      +materialized: view          # intermediate en view

    marts:                         # s'applique à models/marts/**
      +materialized: table         # marts en table pour la BI
      +tags: ['marts']

      finance:                     # s'applique à models/marts/finance/**
        +schema: finance           # schema dédié en prod

      marketing:                   # s'applique à models/marts/marketing/**
        +schema: marketing

# ═══════════════════════════════════════════════════════
# CONFIGURATION DES SEEDS
# ═══════════════════════════════════════════════════════
seeds:
  mon_projet_dbt:
    +schema: reference             # schema dédié pour les seeds

# ═══════════════════════════════════════════════════════
# CONFIGURATION DES TESTS
# ═══════════════════════════════════════════════════════
tests:
  mon_projet_dbt:
    +severity: error               # les tests échouent en erreur par défaut
    +store_failures: true          # stocker les lignes en échec dans le warehouse

# ═══════════════════════════════════════════════════════
# VARIABLES GLOBALES
# ═══════════════════════════════════════════════════════
vars:
  start_date: '2024-01-01'
  is_ci: false

# ═══════════════════════════════════════════════════════
# HOOKS (SQL exécuté avant/après un run)
# ═══════════════════════════════════════════════════════
on-run-end:
  - "{{ log('Run terminé avec succès', info=true) }}"

# ═══════════════════════════════════════════════════════
# CONTRAINTE DE VERSION dbt
# ═══════════════════════════════════════════════════════
require-dbt-version: [">=1.7.0", "<2.0.0"]   # version min et max supportées

# ═══════════════════════════════════════════════════════
# CONFIGURATION dbt CLOUD (seulement si vous utilisez dbt Cloud)
# ═══════════════════════════════════════════════════════
# dbt-cloud:
#   project-id: 12345
#   defer-env-id: 67890
```

---

#### `profiles.yml` — Connexion au warehouse

**Rôle** : contient toutes les informations de connexion à votre data warehouse. C'est le fichier qui fait le lien entre dbt et la base de données où les transformations seront exécutées.

**Emplacement** : par défaut, dbt cherche ce fichier dans `~/.dbt/profiles.yml` (votre répertoire home). C'est voulu pour **ne pas le committer dans Git** — il contient des informations sensibles (mots de passe, chemins de keyfiles). Vous pouvez changer l'emplacement avec la variable d'environnement `DBT_PROFILES_DIR`.

**Mécanisme interne** : le champ `profile` dans `dbt_project.yml` doit correspondre à une clé de premier niveau dans `profiles.yml`. dbt utilise le `target` par défaut (champ `target:`) sauf si vous le surchargez avec `--target`.

```yaml
# ~/.dbt/profiles.yml (emplacement par défaut — hors du repo Git)

mon_projet_dbt:                  # ← doit correspondre à 'profile' dans dbt_project.yml
  target: dev                     # target utilisé par défaut quand on lance 'dbt run'

  outputs:                        # liste des targets disponibles
    dev:                          # ── DÉVELOPPEMENT ──
      type: bigquery              # chaque target peut cibler un warehouse différent
      method: oauth
      project: mon-projet-gcp-dev
      dataset: "dbt_{{ env_var('USER', 'default') }}"  # schema unique par développeur
      threads: 4                  # nombre de modèles exécutés en parallèle
      location: EU

    staging:                      # ── STAGING / PREPROD ──
      type: bigquery
      method: service-account
      project: mon-projet-gcp-staging
      dataset: dbt_staging
      threads: 8
      keyfile: "{{ env_var('DBT_GOOGLE_KEYFILE') }}"   # keyfile via variable d'env
      location: EU

    prod:                         # ── PRODUCTION ──
      type: bigquery
      method: service-account
      project: mon-projet-gcp-prod
      dataset: dbt_prod
      threads: 16
      keyfile: "{{ env_var('DBT_GOOGLE_KEYFILE_PROD') }}"
      location: EU

    ci:                           # ── CI/CD ──
      type: bigquery
      method: service-account
      project: mon-projet-gcp-dev
      dataset: "dbt_ci_{{ env_var('GITHUB_RUN_ID', 'local') }}"  # schema unique par run CI
      threads: 4
      keyfile: "{{ env_var('DBT_GOOGLE_KEYFILE') }}"
      location: EU
```

```bash
# Utiliser un target spécifique (surcharge le target par défaut)
dbt run --target prod              # exécute sur production
dbt run --target staging           # exécute sur staging
dbt run                            # utilise le target par défaut (dev)
```

**Bonnes pratiques sécurité** :
- Ne **jamais** committer `profiles.yml` dans Git (ajoutez-le dans `.gitignore`)
- Utiliser `{{ env_var('VAR_NAME') }}` pour les mots de passe et chemins de keyfiles
- En CI/CD, passer les secrets via les GitHub Secrets ou votre gestionnaire de secrets
- Chaque développeur a son propre schema dev (ex : `dbt_jean`, `dbt_marie`)

---

#### `packages.yml` — Dépendances

```
packages.yml             # à la racine du projet, versionné dans Git
```

**Rôle** : déclare les **packages dbt externes** à installer. Un package dbt est un projet dbt réutilisable contenant des macros, des tests et parfois des modèles. C'est l'équivalent de `package.json` (Node.js) ou `requirements.txt` (Python) pour dbt.

**Mécanisme interne** : quand vous lancez `dbt deps`, dbt lit `packages.yml`, télécharge chaque package (depuis le hub dbt, un dépôt Git, ou un chemin local), et les installe dans le dossier `dbt_packages/`. Les macros et tests de ces packages deviennent alors disponibles dans votre projet.

```yaml
# packages.yml — Exemple complet

packages:
  # ─── Packages depuis le Hub dbt (hub.getdbt.com) ───
  - package: dbt-labs/dbt_utils                   # boîte à outils incontournable
    version: [">=1.0.0", "<2.0.0"]                # contrainte de version sémantique
    # Contient : surrogate_key, pivot, unpivot, date_spine, union_relations...

  - package: calogica/dbt_expectations             # tests avancés (style Great Expectations)
    version: [">=0.10.0", "<0.11.0"]
    # Contient : expect_column_to_exist, expect_values_to_be_between,
    #            expect_column_values_to_be_of_type...

  - package: dbt-labs/codegen                      # génération de code
    version: [">=0.12.0", "<0.13.0"]
    # Contient : generate_source (crée le YAML de sources automatiquement),
    #            generate_model_yaml (crée le schema.yml d'un modèle)

  - package: dbt-labs/audit_helper                 # audit et comparaison
    version: [">=0.9.0", "<0.10.0"]
    # Contient : compare_relations (compare deux tables colonne par colonne)

  # ─── Package depuis un dépôt Git privé ───
  - git: "https://github.com/mon-org/dbt-internal-utils.git"
    revision: v2.1.0                               # tag Git, branche, ou SHA de commit

  # ─── Package depuis un chemin local (dev) ───
  # - local: ../mon-autre-package
```

```bash
# Installer les packages (à lancer après chaque modification de packages.yml)
dbt deps

# Les packages sont installés dans dbt_packages/ (ajoutez-le au .gitignore)
```

**Résultat dans l'arborescence** :

```
dbt_packages/                        # créé par dbt deps (ne pas committer)
  dbt_utils/                         # le package installé
    macros/                          # les macros deviennent disponibles
      generate_surrogate_key.sql
      pivot.sql
      ...
    dbt_project.yml                  # chaque package est un projet dbt complet
  dbt_expectations/
    ...
```

**Usage des macros de packages dans vos modèles** :

```sql
-- Pas besoin d'import — les macros sont automatiquement disponibles après dbt deps
select
    {{ dbt_utils.generate_surrogate_key(['order_id', 'product_id']) }} as line_id,
    order_id,
    product_id,
    quantity
from {{ ref('stg_order_lines') }}
```

---

#### Fichiers complémentaires

##### `selectors.yml` — Groupes de sélection nommés (optionnel)

**Rôle** : permet de créer des **sélections nommées et réutilisables** de modèles. Au lieu de taper une commande de sélection complexe à chaque fois, vous la nommez une fois et l'utilisez avec `--selector`.

```yaml
# selectors.yml
selectors:
  - name: nightly_build
    description: "Tous les modèles à exécuter chaque nuit"
    definition:
      union:
        - "tag:nightly"
        - "path:models/marts"
        - exclude:
            - "tag:experimental"
```

```bash
# Au lieu de taper la sélection complète
dbt build --selector nightly_build
```

##### `.gitignore` — Fichiers à exclure de Git

```gitignore
# .gitignore pour un projet dbt

# Artefacts de compilation — régénérés à chaque run
target/
logs/

# Packages installés — régénérés par dbt deps
dbt_packages/

# Connexion au warehouse — contient des secrets
profiles.yml

# Fichiers système
.DS_Store
__pycache__/
*.pyc

# Environnements virtuels Python
.venv/
venv/
```


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

### 8.1 Qu'est-ce que Jinja dans dbt ?

Jinja est un moteur de templating Python intégré nativement dans dbt. Il transforme vos fichiers SQL en **templates dynamiques** capables de générer du SQL différent selon le contexte (environnement, variables, conditions). Quand vous exécutez `dbt run` ou `dbt compile`, dbt commence par résoudre tout le Jinja, puis envoie le SQL pur résultant au warehouse.

Le cycle est donc : **Jinja + SQL → compilation → SQL pur → exécution sur le warehouse**.

Vous pouvez voir le SQL compilé (sans Jinja) dans le dossier `target/compiled/` après un `dbt compile`.

### 8.2 Les 3 types de blocs Jinja

```sql
-- ═══════════════════════════════════════════════════════════════
-- TYPE 1 : EXPRESSIONS  {{ ... }}
-- Rôle : évaluer et afficher une valeur dans le SQL généré
-- ═══════════════════════════════════════════════════════════════

-- Appeler une fonction dbt (ref, source, var, etc.)
select * from {{ ref('stg_orders') }}
-- Compilé en → select * from analytics.dbt_dev.stg_orders

-- Afficher une variable
where order_date >= '{{ var("start_date") }}'
-- Compilé en → where order_date >= '2024-01-01'

-- Appeler une macro personnalisée
select {{ cents_to_euros('amount_cents') }} as amount_euros
-- Compilé en → select round(amount_cents / 100.0, 2) as amount_euros

-- Afficher le nom du target courant
-- {{ target.name }}  → 'dev', 'staging', ou 'prod'

-- Afficher le schema courant
-- {{ target.schema }} → 'dbt_dev', 'dbt_prod', etc.


-- ═══════════════════════════════════════════════════════════════
-- TYPE 2 : INSTRUCTIONS  {% ... %}
-- Rôle : logique de contrôle (conditions, boucles, variables)
-- Ces blocs ne produisent aucun texte dans le SQL final
-- ═══════════════════════════════════════════════════════════════

{% set my_variable = 42 %}               -- déclarer une variable
{% if condition %}...{% endif %}          -- condition
{% for item in list %}...{% endfor %}    -- boucle

-- IMPORTANT : les instructions ne génèrent PAS de texte
-- Seul le contenu entre les blocs est inclus dans le SQL


-- ═══════════════════════════════════════════════════════════════
-- TYPE 3 : COMMENTAIRES  {# ... #}
-- Rôle : commentaires Jinja, invisibles dans le SQL compilé
-- Différent des commentaires SQL (-- ...) qui eux restent
-- ═══════════════════════════════════════════════════════════════

{# Ce commentaire n'apparaît PAS dans le SQL compilé #}
-- Ce commentaire SQL apparaît dans le SQL compilé

{# 
   Les commentaires Jinja peuvent
   s'étendre sur plusieurs lignes
#}
```

### 8.3 Variables Jinja — `set`

Les variables Jinja permettent de stocker des valeurs pour les réutiliser dans le modèle.

```sql
-- ═══════════════════════════════════════════════════════════════
-- VARIABLES SIMPLES
-- ═══════════════════════════════════════════════════════════════

-- Déclarer une variable simple
{% set tva_rate = 0.20 %}

select
    order_id,
    amount_ht,                                              -- montant hors taxe
    amount_ht * {{ tva_rate }} as tva_amount,               -- montant TVA (20%)
    amount_ht * (1 + {{ tva_rate }}) as amount_ttc          -- montant TTC
from {{ ref('stg_orders') }}
-- Compilé en →
-- amount_ht * 0.2 as tva_amount,
-- amount_ht * (1 + 0.2) as amount_ttc


-- ═══════════════════════════════════════════════════════════════
-- VARIABLES LISTES
-- ═══════════════════════════════════════════════════════════════

-- Déclarer une liste
{% set status_list = ['pending', 'shipped', 'completed'] %}

select *
from {{ ref('stg_orders') }}
where order_status in (
    {% for status in status_list %}
        '{{ status }}'{% if not loop.last %},{% endif %}    -- virgule entre chaque, sauf le dernier
    {% endfor %}
)
-- Compilé en →
-- where order_status in ('pending', 'shipped', 'completed')


-- ═══════════════════════════════════════════════════════════════
-- VARIABLES DICTIONNAIRES
-- ═══════════════════════════════════════════════════════════════

-- Déclarer un dictionnaire
{% set column_mapping = {
    'usr_id': 'customer_id',
    'ord_dt': 'order_date',
    'amt': 'amount_euros'
} %}

select
    {% for old_name, new_name in column_mapping.items() %}
        {{ old_name }} as {{ new_name }}{% if not loop.last %},{% endif %}
    {% endfor %}
from {{ source('legacy', 'orders') }}
-- Compilé en →
-- usr_id as customer_id,
-- ord_dt as order_date,
-- amt as amount_euros
```

### 8.4 Conditions — `if / elif / else`

```sql
-- ═══════════════════════════════════════════════════════════════
-- CONDITION SIMPLE : adapter selon l'environnement
-- ═══════════════════════════════════════════════════════════════

select *
from {{ ref('stg_orders') }}

{% if target.name == 'dev' %}
    -- En dev : limiter aux 3 derniers mois pour accélérer le développement
    where order_date >= dateadd('month', -3, current_date)

{% elif target.name == 'staging' %}
    -- En staging : limiter à 1 an pour tester sur un volume réaliste
    where order_date >= dateadd('year', -1, current_date)

{% endif %}
-- En prod (pas de condition matchée) : aucun filtre → toutes les données


-- ═══════════════════════════════════════════════════════════════
-- CONDITION AVEC VARIABLE : activer/désactiver des fonctionnalités
-- ═══════════════════════════════════════════════════════════════

-- La variable 'include_cancelled' peut être passée via --vars
{% set include_cancelled = var('include_cancelled', false) %}

select *
from {{ ref('stg_orders') }}

{% if not include_cancelled %}
    where order_status != 'cancelled'    -- exclure les annulées par défaut
{% endif %}

-- Appel avec : dbt run --vars '{"include_cancelled": true}'


-- ═══════════════════════════════════════════════════════════════
-- CONDITION SUR LE TYPE DE WAREHOUSE (adapter le SQL)
-- ═══════════════════════════════════════════════════════════════

select
    order_id,

    -- La fonction de calcul de différence de dates varie selon le warehouse
    {% if target.type == 'bigquery' %}
        date_diff(dropoff_datetime, pickup_datetime, minute)
    {% elif target.type == 'snowflake' %}
        datediff('minute', pickup_datetime, dropoff_datetime)
    {% elif target.type == 'postgres' %}
        extract(epoch from (dropoff_datetime - pickup_datetime)) / 60
    {% elif target.type == 'duckdb' %}
        date_diff('minute', pickup_datetime, dropoff_datetime)
    {% else %}
        -- Fallback générique
        datediff('minute', pickup_datetime, dropoff_datetime)
    {% endif %}
    as trip_duration_minutes

from {{ ref('stg_trips') }}


-- ═══════════════════════════════════════════════════════════════
-- is_incremental() : condition spéciale pour les modèles incrémentaux
-- ═══════════════════════════════════════════════════════════════

{{
  config(
    materialized='incremental',
    unique_key='event_id'
  )
}}

select *
from {{ source('analytics', 'events') }}

{% if is_incremental() %}
    -- Cette condition est vraie UNIQUEMENT si :
    -- 1. Le modèle existe déjà dans le warehouse
    -- 2. On n'est PAS en mode --full-refresh
    -- Elle filtre pour ne traiter que les nouvelles données
    where event_timestamp > (
        select max(event_timestamp) from {{ this }}   -- {{ this }} = la table actuelle
    )
{% endif %}
-- Premier run : is_incremental() = false → charge TOUT
-- Runs suivants : is_incremental() = true → charge seulement le nouveau
```

### 8.5 Boucles — `for`

```sql
-- ═══════════════════════════════════════════════════════════════
-- BOUCLE SIMPLE : générer des colonnes dynamiquement
-- ═══════════════════════════════════════════════════════════════

{% set payment_methods = ['credit_card', 'cash', 'bank_transfer', 'voucher'] %}

select
    order_id,

    -- Génère une colonne par méthode de paiement
    {% for method in payment_methods %}
        sum(
            case
                when payment_method = '{{ method }}'
                then amount
                else 0
            end
        ) as {{ method }}_amount                           -- ex: credit_card_amount

        {% if not loop.last %},{% endif %}                 -- virgule sauf après le dernier
    {% endfor %}

from {{ ref('stg_payments') }}
group by order_id

-- Compilé en →
-- sum(case when payment_method = 'credit_card' then amount else 0 end) as credit_card_amount,
-- sum(case when payment_method = 'cash' then amount else 0 end) as cash_amount,
-- sum(case when payment_method = 'bank_transfer' then amount else 0 end) as bank_transfer_amount,
-- sum(case when payment_method = 'voucher' then amount else 0 end) as voucher_amount


-- ═══════════════════════════════════════════════════════════════
-- BOUCLE AVEC INDEX : utiliser loop.index
-- ═══════════════════════════════════════════════════════════════

{% set months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'] %}

select
    year,
    {% for month in months %}
        -- loop.index commence à 1 (loop.index0 commence à 0)
        sum(case when extract(month from order_date) = {{ loop.index }}
            then amount else 0 end) as revenue_{{ month | lower }}
        {% if not loop.last %},{% endif %}
    {% endfor %}
from {{ ref('stg_orders') }}
group by year


-- ═══════════════════════════════════════════════════════════════
-- BOUCLE SUR UN DICTIONNAIRE : renommer des colonnes
-- ═══════════════════════════════════════════════════════════════

{% set renames = {
    'cust_id': 'customer_id',
    'ord_date': 'order_date',
    'prod_name': 'product_name',
    'qty': 'quantity',
    'unit_px': 'unit_price'
} %}

select
    {% for old_col, new_col in renames.items() %}
        {{ old_col }} as {{ new_col }}{% if not loop.last %},{% endif %}
    {% endfor %}
from {{ source('legacy_system', 'orders') }}


-- ═══════════════════════════════════════════════════════════════
-- BOUCLE AVEC UNION ALL : combiner plusieurs sources similaires
-- ═══════════════════════════════════════════════════════════════

{% set years = [2022, 2023, 2024] %}

{% for year in years %}
    select
        *,
        {{ year }} as source_year                          -- ajouter l'année d'origine
    from {{ source('archive', 'orders_' ~ year) }}         -- ~ = concaténation Jinja

    {% if not loop.last %}
    union all                                              -- union entre chaque année
    {% endif %}
{% endfor %}
-- Compilé en →
-- select *, 2022 as source_year from archive.orders_2022
-- union all
-- select *, 2023 as source_year from archive.orders_2023
-- union all
-- select *, 2024 as source_year from archive.orders_2024


-- ═══════════════════════════════════════════════════════════════
-- PROPRIÉTÉS DE loop DISPONIBLES DANS UNE BOUCLE
-- ═══════════════════════════════════════════════════════════════

{# 
   loop.index      → itération courante (commence à 1)
   loop.index0     → itération courante (commence à 0)
   loop.first      → true si c'est la première itération
   loop.last       → true si c'est la dernière itération
   loop.length     → nombre total d'itérations
   loop.revindex   → itérations restantes (commence à length, finit à 1)
#}
```

### 8.6 Filtres Jinja

Les filtres transforment une valeur. Ils s'appliquent avec le pipe `|`.

```sql
-- ═══════════════════════════════════════════════════════════════
-- FILTRES LES PLUS UTILES DANS dbt
-- ═══════════════════════════════════════════════════════════════

{% set my_name = "Hello World" %}

-- Changement de casse
{{ my_name | lower }}          -- → "hello world"
{{ my_name | upper }}          -- → "HELLO WORLD"
{{ my_name | capitalize }}     -- → "Hello world"
{{ my_name | title }}          -- → "Hello World"

-- Remplacement
{{ my_name | replace("World", "dbt") }}  -- → "Hello dbt"

-- Suppression des espaces
{% set padded = "  hello  " %}
{{ padded | trim }}            -- → "hello"

-- Valeur par défaut si la variable est undefined ou None
{{ my_undefined_var | default("fallback_value") }}

-- Conversion en string
{{ 42 | string }}              -- → "42"

-- Conversion en entier
{{ "42" | int }}               -- → 42

-- Jointure d'une liste en string
{% set cols = ['a', 'b', 'c'] %}
{{ cols | join(', ') }}        -- → "a, b, c"

-- Longueur d'une liste ou string
{{ cols | length }}            -- → 3


-- ═══════════════════════════════════════════════════════════════
-- EXEMPLE PRATIQUE : filtres dans un modèle
-- ═══════════════════════════════════════════════════════════════

{% set columns_to_select = ['order_id', 'customer_id', 'amount', 'order_date'] %}

select
    {{ columns_to_select | join(',\n    ') }}              -- joint avec virgule + retour à la ligne
from {{ ref('stg_orders') }}

-- Compilé en →
-- select
--     order_id,
--     customer_id,
--     amount,
--     order_date
-- from ...
```

### 8.7 Les espaces blancs — contrôle avec `-`

Jinja génère parfois des lignes vides indésirables dans le SQL compilé. Le tiret `-` permet de supprimer les espaces blancs.

```sql
-- ═══════════════════════════════════════════════════════════════
-- PROBLÈME : lignes vides dans le SQL compilé
-- ═══════════════════════════════════════════════════════════════

-- SANS contrôle des espaces (génère des lignes vides)
{% set limit_rows = 100 %}
select *
from {{ ref('stg_orders') }}
{% if target.name == 'dev' %}
limit {{ limit_rows }}
{% endif %}
-- Compilé en (notez les lignes vides) →
-- select *
-- from analytics.dbt_dev.stg_orders
--
-- limit 100
--


-- AVEC contrôle des espaces (propre)
{%- set limit_rows = 100 -%}                              -- tiret = supprime les espaces autour
select *
from {{ ref('stg_orders') }}
{%- if target.name == 'dev' %}
limit {{ limit_rows }}
{%- endif %}
-- Compilé en (propre) →
-- select *
-- from analytics.dbt_dev.stg_orders
-- limit 100


-- ═══════════════════════════════════════════════════════════════
-- RÈGLE GÉNÉRALE
-- ═══════════════════════════════════════════════════════════════

{# 
   {%- ... %}   → supprime les espaces AVANT le bloc
   {% ... -%}   → supprime les espaces APRÈS le bloc
   {%- ... -%}  → supprime les espaces AVANT et APRÈS
   
   Même logique pour les expressions :
   {{- ... }}   → supprime avant
   {{ ... -}}   → supprime après
   {{- ... -}}  → supprime avant et après
#}
```

### 8.8 Créer des macros — Les fondamentaux

Une macro est une **fonction réutilisable** écrite en Jinja. Elle vit dans le dossier `macros/` et peut être appelée depuis n'importe quel modèle, test ou autre macro.

```sql
-- ═══════════════════════════════════════════════════════════════
-- MACRO SIMPLE : conversion de devises
-- Fichier : macros/cents_to_euros.sql
-- ═══════════════════════════════════════════════════════════════

{% macro cents_to_euros(column_name, precision=2) %}
    {# 
       Convertit une colonne de centimes en euros.
       
       Arguments :
         - column_name (str) : nom de la colonne en centimes
         - precision (int)   : nombre de décimales (défaut : 2)
       
       Retourne : expression SQL arrondie en euros
       
       Exemple d'appel :
         {{ cents_to_euros('price_cents') }}
         → round(price_cents / 100.0, 2)
    #}
    round({{ column_name }} / 100.0, {{ precision }})
{% endmacro %}
```

```sql
-- Utilisation dans un modèle :
-- models/staging/stg_orders.sql

select
    order_id,
    {{ cents_to_euros('amount_cents') }} as amount_euros,          -- → round(amount_cents / 100.0, 2)
    {{ cents_to_euros('shipping_cents') }} as shipping_euros,      -- → round(shipping_cents / 100.0, 2)
    {{ cents_to_euros('tax_cents', 4) }} as tax_euros              -- → round(tax_cents / 100.0, 4)
from {{ source('ecommerce', 'raw_orders') }}
```

```sql
-- ═══════════════════════════════════════════════════════════════
-- MACRO AVEC LOGIQUE CONDITIONNELLE
-- Fichier : macros/safe_divide.sql
-- ═══════════════════════════════════════════════════════════════

{% macro safe_divide(numerator, denominator, default_value=0) %}
    {# 
       Division sécurisée : retourne default_value si le dénominateur est 0 ou NULL.
       Évite les erreurs "division by zero".
       
       Arguments :
         - numerator (str)     : expression du numérateur
         - denominator (str)   : expression du dénominateur
         - default_value       : valeur de remplacement (défaut : 0)
    #}
    case
        when {{ denominator }} is null or {{ denominator }} = 0
        then {{ default_value }}
        else {{ numerator }} / nullif({{ denominator }}, 0)
    end
{% endmacro %}
```

```sql
-- Utilisation :
select
    customer_id,
    total_revenue,
    total_orders,
    {{ safe_divide('total_revenue', 'total_orders') }} as avg_order_value,
    -- → case when total_orders is null or total_orders = 0 then 0
    --        else total_revenue / nullif(total_orders, 0) end

    {{ safe_divide('completed_orders', 'total_orders', 'null') }} as completion_rate
    -- → ...then null else completed_orders / nullif(total_orders, 0) end
from {{ ref('fct_revenue') }}
```

### 8.9 Macros avancées

```sql
-- ═══════════════════════════════════════════════════════════════
-- MACRO : générer un schema dynamique
-- Fichier : macros/generate_schema_name.sql
-- ═══════════════════════════════════════════════════════════════

-- Cette macro SURCHARGE la macro native de dbt
-- Elle contrôle comment les noms de schema sont construits
-- Comportement :
--   - En prod : utilise le custom_schema_name tel quel (ex: "finance")
--   - En dev  : préfixe avec le schema personnel (ex: "dbt_jean_finance")

{% macro generate_schema_name(custom_schema_name, node) %}

    {%- set default_schema = target.schema -%}              -- ex: "dbt_dev" ou "dbt_jean"

    {%- if custom_schema_name is none -%}
        {# Pas de schema custom configuré → utiliser le schema par défaut #}
        {{ default_schema }}

    {%- elif target.name == 'prod' -%}
        {# En production → utiliser le schema custom directement #}
        {# Résultat : "finance", "marketing", etc. #}
        {{ custom_schema_name | trim }}

    {%- else -%}
        {# En dev/staging → préfixer pour isoler chaque développeur #}
        {# Résultat : "dbt_jean_finance", "dbt_jean_marketing", etc. #}
        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{% endmacro %}


-- ═══════════════════════════════════════════════════════════════
-- MACRO : limiter les données en CI
-- Fichier : macros/limit_for_ci.sql
-- ═══════════════════════════════════════════════════════════════

{% macro limit_for_ci(limit_count=1000) %}
    {# 
       Ajoute un LIMIT en mode CI pour réduire les coûts.
       Activé via : dbt run --vars '{"is_ci": true}'
       
       Arguments :
         - limit_count (int) : nombre de lignes max en CI (défaut : 1000)
    #}
    {%- if var('is_ci', false) -%}
        limit {{ limit_count }}
    {%- endif -%}
{% endmacro %}
```

```sql
-- Utilisation dans un modèle :
select *
from {{ ref('stg_events') }}
where event_date >= '2024-01-01'
{{ limit_for_ci() }}                    -- ajoute "limit 1000" uniquement en CI
-- En CI  → ...where event_date >= '2024-01-01' limit 1000
-- En dev → ...where event_date >= '2024-01-01'
```

```sql
-- ═══════════════════════════════════════════════════════════════
-- MACRO : générer des tests de fraîcheur custom
-- Fichier : macros/assert_row_count_above.sql
-- ═══════════════════════════════════════════════════════════════

{% macro assert_row_count_above(model, min_rows) %}
    {# 
       Retourne les résultats si le modèle a moins de min_rows lignes.
       À utiliser comme test singulier ou via run-operation.
    #}
    with row_count as (
        select count(*) as cnt
        from {{ model }}
    )
    select cnt
    from row_count
    where cnt < {{ min_rows }}
{% endmacro %}
```

```sql
-- ═══════════════════════════════════════════════════════════════
-- MACRO : pivoter des colonnes (version simplifiée de dbt_utils.pivot)
-- Fichier : macros/simple_pivot.sql
-- ═══════════════════════════════════════════════════════════════

{% macro simple_pivot(column, values, agg='sum', value_column='amount') %}
    {# 
       Pivot simple : transforme des lignes en colonnes.
       
       Arguments :
         - column (str)        : colonne contenant les valeurs à pivoter
         - values (list)       : liste des valeurs distinctes attendues
         - agg (str)           : fonction d'agrégation (sum, count, avg, max, min)
         - value_column (str)  : colonne à agréger
       
       Exemple :
         {{ simple_pivot('status', ['pending', 'completed', 'cancelled']) }}
         
       Génère :
         sum(case when status = 'pending' then amount else 0 end) as pending_amount,
         sum(case when status = 'completed' then amount else 0 end) as completed_amount,
         sum(case when status = 'cancelled' then amount else 0 end) as cancelled_amount
    #}

    {% for value in values %}
        {{ agg }}(
            case
                when {{ column }} = '{{ value }}'
                then {{ value_column }}
                else 0
            end
        ) as {{ value }}_{{ value_column }}
        {%- if not loop.last %},{% endif %}
    {% endfor %}

{% endmacro %}
```

```sql
-- Utilisation :
select
    customer_id,
    {{ simple_pivot(
        column='order_status',
        values=['pending', 'completed', 'cancelled'],
        agg='count',
        value_column='order_id'
    ) }}
from {{ ref('stg_orders') }}
group by customer_id

-- Compilé en →
-- count(case when order_status = 'pending' then order_id else 0 end) as pending_order_id,
-- count(case when order_status = 'completed' then order_id else 0 end) as completed_order_id,
-- count(case when order_status = 'cancelled' then order_id else 0 end) as cancelled_order_id
```

### 8.10 Macros d'administration — `run-operation`

Certaines macros ne sont pas appelées dans des modèles mais exécutées directement via la commande `dbt run-operation`.

```sql
-- ═══════════════════════════════════════════════════════════════
-- MACRO : supprimer les schemas temporaires de CI
-- Fichier : macros/drop_ci_schemas.sql
-- Appel : dbt run-operation drop_ci_schemas
-- ═══════════════════════════════════════════════════════════════

{% macro drop_ci_schemas() %}
    {# 
       Supprime tous les schemas créés par la CI.
       Convention : les schemas CI sont préfixés par "dbt_ci_".
       
       Utilisé dans le pipeline CI (GitHub Actions) en step de nettoyage.
    #}

    {% set schemas_query %}
        -- Requête pour trouver tous les schemas CI
        select schema_name
        from information_schema.schemata
        where schema_name like 'dbt_ci_%'
    {% endset %}

    -- Exécuter la requête et récupérer les résultats
    {% set results = run_query(schemas_query) %}

    {% if execute %}
        {# execute est true uniquement lors de l'exécution réelle (pas à la compilation) #}
        {% for row in results %}
            {% set drop_query = "drop schema if exists " ~ row['schema_name'] ~ " cascade" %}
            {{ log("Dropping schema: " ~ row['schema_name'], info=true) }}
            {% do run_query(drop_query) %}
        {% endfor %}
    {% endif %}

{% endmacro %}
```

```bash
# Exécution directe de la macro
dbt run-operation drop_ci_schemas
```

```sql
-- ═══════════════════════════════════════════════════════════════
-- MACRO : accorder des permissions de lecture
-- Fichier : macros/grant_select.sql
-- Appel : dbt run-operation grant_select --args '{"schema": "marts", "role": "analyst"}'
-- ═══════════════════════════════════════════════════════════════

{% macro grant_select(schema, role) %}
    {# 
       Accorde le SELECT sur toutes les tables d'un schema à un rôle.
       
       Arguments :
         - schema (str) : nom du schema
         - role (str)   : nom du rôle à autoriser
    #}

    {% set grant_query %}
        grant usage on schema {{ schema }} to role {{ role }};
        grant select on all tables in schema {{ schema }} to role {{ role }};
    {% endset %}

    {{ log("Granting SELECT on " ~ schema ~ " to " ~ role, info=true) }}
    {% do run_query(grant_query) %}

{% endmacro %}
```

### 8.11 Les hooks — Exécuter du SQL avant/après

Les hooks permettent d'exécuter du SQL automatiquement avant ou après un modèle, ou au début/fin d'un run complet.

```yaml
# ═══════════════════════════════════════════════════════════════
# HOOKS AU NIVEAU DU PROJET — dbt_project.yml
# ═══════════════════════════════════════════════════════════════

# Exécuté UNE FOIS au début de chaque dbt run/build/test
on-run-start:
  - "{{ log('Starting dbt run at ' ~ modules.datetime.datetime.now(), info=true) }}"

# Exécuté UNE FOIS à la fin de chaque dbt run/build/test
on-run-end:
  - "grant select on all tables in schema {{ target.schema }} to role analyst"
  - "{{ log('Finished dbt run', info=true) }}"
```

```sql
-- ═══════════════════════════════════════════════════════════════
-- HOOKS AU NIVEAU D'UN MODÈLE — dans le config()
-- ═══════════════════════════════════════════════════════════════

{{
  config(
    materialized='table',

    -- Exécuté AVANT la création de ce modèle
    pre_hook=[
      "{{ log('Building fct_revenue...', info=true) }}"
    ],

    -- Exécuté APRÈS la création de ce modèle
    post_hook=[
      -- Accorder les droits de lecture aux analystes
      "grant select on {{ this }} to role analyst",

      -- Créer un index pour améliorer les performances de requête
      "create index if not exists idx_customer on {{ this }} (customer_id)"
    ]
  )
}}

select
    customer_id,
    sum(amount) as total_revenue
from {{ ref('stg_orders') }}
group by customer_id
```

```yaml
# ═══════════════════════════════════════════════════════════════
# HOOKS AU NIVEAU D'UN DOSSIER — dbt_project.yml
# ═══════════════════════════════════════════════════════════════

models:
  mon_projet:
    marts:
      # Tous les modèles du dossier marts auront ce post-hook
      +post-hook:
        - "grant select on {{ this }} to role analyst"
```

### 8.12 Variables dbt — `var()`

Les variables dbt sont déclarées dans `dbt_project.yml` et accessibles partout avec `{{ var() }}`.

```yaml
# ═══════════════════════════════════════════════════════════════
# DÉCLARATION DES VARIABLES — dbt_project.yml
# ═══════════════════════════════════════════════════════════════

vars:
  # Variables globales (accessibles par tout le projet et tous les packages)
  start_date: '2024-01-01'
  default_currency: 'EUR'
  is_ci: false                        # surchargé en CI via --vars

  # Variables spécifiques au projet (inaccessibles par les packages)
  mon_projet:
    tax_rate: 0.20
    excluded_countries: ['XX', 'ZZ']

  # Variables spécifiques à un package installé
  dbt_utils:
    surrogate_key_treat_nulls_as_empty_strings: true
```

```sql
-- ═══════════════════════════════════════════════════════════════
-- UTILISATION DANS UN MODÈLE
-- ═══════════════════════════════════════════════════════════════

select *
from {{ ref('stg_orders') }}
where
    -- var() avec valeur par défaut (recommandé pour la robustesse)
    order_date >= '{{ var("start_date", "2024-01-01") }}'

    -- var() sans valeur par défaut (erreur si la variable n'existe pas)
    and currency = '{{ var("default_currency") }}'
```

```bash
# Surcharger une variable en ligne de commande
dbt run --vars '{"start_date": "2023-01-01", "is_ci": true}'

# Surcharger plusieurs variables
dbt run --vars '{"start_date": "2023-06-01", "excluded_countries": ["FR", "DE"]}'
```

### 8.13 Objets contextuels disponibles dans Jinja

dbt met à disposition plusieurs objets contextuels dans le Jinja :

```sql
-- ═══════════════════════════════════════════════════════════════
-- L'OBJET target — informations sur l'environnement courant
-- ═══════════════════════════════════════════════════════════════

{# 
   target.name      → nom du target ('dev', 'staging', 'prod')
   target.schema    → schema cible ('dbt_dev', 'dbt_prod')
   target.database  → base de données cible
   target.type      → type de warehouse ('bigquery', 'snowflake', 'postgres')
   target.threads   → nombre de threads configurés
   target.profile_name → nom du profil utilisé
#}

-- Exemple : un commentaire SQL avec le contexte d'exécution
-- Environnement : {{ target.name }}, Schema : {{ target.schema }}
select * from {{ ref('stg_orders') }}


-- ═══════════════════════════════════════════════════════════════
-- L'OBJET this — référence au modèle courant (pour les incrémentaux)
-- ═══════════════════════════════════════════════════════════════

{# 
   {{ this }}          → nom complet de la table actuelle (database.schema.table)
   {{ this.schema }}   → schema de la table actuelle
   {{ this.name }}     → nom de la table actuelle
   {{ this.database }} → base de données de la table actuelle
#}

-- Utilisé principalement dans les modèles incrémentaux
{% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
{% endif %}


-- ═══════════════════════════════════════════════════════════════
-- L'OBJET model — métadonnées du modèle courant
-- ═══════════════════════════════════════════════════════════════

{# 
   model.name         → nom du modèle (ex: 'stg_orders')
   model.tags         → liste des tags du modèle
   model.config       → configuration du modèle
   model.description  → description du modèle
   model.path         → chemin du fichier
#}


-- ═══════════════════════════════════════════════════════════════
-- LES FONCTIONS UTILITAIRES
-- ═══════════════════════════════════════════════════════════════

{#
   ref('model_name')                  → référence un autre modèle
   source('source', 'table')          → référence une source
   var('name', default)               → accède à une variable
   env_var('ENV_VAR_NAME', default)   → lit une variable d'environnement
   is_incremental()                   → true si le modèle est en mode incrémental
   log('message', info=true)          → écrit dans les logs dbt
   run_query('sql')                   → exécute une requête et retourne les résultats
   adapter.dispatch('macro')()        → appelle une macro spécifique à l'adapter
   execute                            → true pendant l'exécution (false pendant la compilation)
   modules.datetime.datetime.now()    → date et heure courantes
   return(value)                      → retourne une valeur depuis une macro
#}
```

### 8.14 `run_query` — Exécuter du SQL dans Jinja

`run_query` permet d'exécuter une requête SQL **pendant la phase de compilation** et d'utiliser les résultats dans le Jinja.

```sql
-- ═══════════════════════════════════════════════════════════════
-- EXEMPLE : récupérer dynamiquement la liste des valeurs distinctes
-- ═══════════════════════════════════════════════════════════════

{# Requête pour obtenir les méthodes de paiement existantes #}
{% set payment_query %}
    select distinct payment_method
    from {{ source('ecommerce', 'orders') }}
    order by payment_method
{% endset %}

{# Exécuter la requête (seulement si on n'est pas en phase de parsing) #}
{% if execute %}
    {% set results = run_query(payment_query) %}
    {% set payment_methods = results.columns[0].values() %}
{% else %}
    {% set payment_methods = [] %}
{% endif %}

{# Utiliser les résultats pour générer du SQL dynamique #}
select
    customer_id,
    {% for method in payment_methods %}
        sum(case when payment_method = '{{ method }}' then amount else 0 end)
            as {{ method | lower | replace(' ', '_') }}_total
        {% if not loop.last %},{% endif %}
    {% endfor %}
from {{ ref('stg_orders') }}
group by customer_id

{# 
   ATTENTION : run_query exécute une requête réelle sur le warehouse.
   - Cela peut être lent si la requête est complexe
   - Protégez toujours avec {% if execute %} pour éviter les erreurs au parsing
   - Préférez les listes statiques ({% set list = [...] %}) quand c'est possible
#}
```

### 8.15 Récapitulatif — Quand utiliser quoi

| Besoin | Solution Jinja | Exemple |
|--------|---------------|---------|
| Référencer un modèle | `{{ ref() }}` | `from {{ ref('stg_orders') }}` |
| Référencer une source | `{{ source() }}` | `from {{ source('ecommerce', 'orders') }}` |
| Adapter au warehouse | `{% if target.type == ... %}` | SQL spécifique BigQuery vs Snowflake |
| Adapter à l'environnement | `{% if target.name == ... %}` | Limiter les données en dev |
| Paramétrer un modèle | `{{ var() }}` | `where date >= '{{ var("start_date") }}'` |
| Lire une variable d'env | `{{ env_var() }}` | `{{ env_var('API_KEY') }}` |
| Réutiliser de la logique SQL | Créer une macro | `{{ cents_to_euros('amount') }}` |
| Générer du SQL répétitif | Boucle `{% for %}` | Colonnes pivotées dynamiquement |
| Gérer l'incrémentalité | `{% if is_incremental() %}` | Filtrer les nouvelles données |
| Exécuter du SQL admin | Macro + `run-operation` | Nettoyage de schemas CI |
| SQL automatique avant/après | Hooks | `post_hook: "grant select ..."` |
| Requêter pendant la compilation | `run_query()` | Récupérer des valeurs dynamiques |
| Factoriser du code SQL | Macro avec paramètres | `{{ safe_divide('a', 'b') }}` |

### 8.16 Erreurs Jinja fréquentes et solutions

```sql
-- ═══════════════════════════════════════════════════════════════
-- ERREUR 1 : "undefined" is undefined
-- ═══════════════════════════════════════════════════════════════
-- Cause : variable ou macro inexistante

-- ❌ Mauvais : la variable n'existe pas et pas de défaut
where date >= '{{ var("star_date") }}'                     -- typo !

-- ✅ Correct : bonne orthographe + valeur par défaut
where date >= '{{ var("start_date", "2024-01-01") }}'


-- ═══════════════════════════════════════════════════════════════
-- ERREUR 2 : virgule en trop dans une boucle
-- ═══════════════════════════════════════════════════════════════
-- Cause : pas de vérification de loop.last

-- ❌ Mauvais : virgule après le dernier élément
{% for col in columns %}
    {{ col }},                                              -- virgule même au dernier !
{% endfor %}

-- ✅ Correct : pas de virgule après le dernier
{% for col in columns %}
    {{ col }}{% if not loop.last %},{% endif %}
{% endfor %}


-- ═══════════════════════════════════════════════════════════════
-- ERREUR 3 : confusion entre = (Jinja) et == (comparaison)
-- ═══════════════════════════════════════════════════════════════

-- ❌ Mauvais : = dans une condition (c'est une assignation)
{% if target.name = 'prod' %}                               -- erreur de syntaxe !

-- ✅ Correct : == pour comparer
{% if target.name == 'prod' %}


-- ═══════════════════════════════════════════════════════════════
-- ERREUR 4 : run_query sans protection execute
-- ═══════════════════════════════════════════════════════════════

-- ❌ Mauvais : erreur au parsing (avant la connexion au warehouse)
{% set results = run_query("select 1") %}

-- ✅ Correct : protégé par execute
{% if execute %}
    {% set results = run_query("select 1") %}
{% endif %}


-- ═══════════════════════════════════════════════════════════════
-- ERREUR 5 : oublier les guillemets autour des strings SQL
-- ═══════════════════════════════════════════════════════════════

{% set status = 'completed' %}

-- ❌ Mauvais : la valeur n'est pas entre guillemets dans le SQL compilé
where order_status = {{ status }}
-- Compilé en → where order_status = completed             -- erreur SQL !

-- ✅ Correct : guillemets autour de la valeur
where order_status = '{{ status }}'
-- Compilé en → where order_status = 'completed'


-- ═══════════════════════════════════════════════════════════════
-- ASTUCE : utiliser dbt compile pour debugger le Jinja
-- ═══════════════════════════════════════════════════════════════

-- Compilez le modèle sans l'exécuter :
-- $ dbt compile --select mon_modele
--
-- Puis consultez le SQL compilé dans :
-- target/compiled/mon_projet/models/.../mon_modele.sql
--
-- Vous verrez le SQL PUR sans aucun Jinja, ce qui facilite le debug.
```


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


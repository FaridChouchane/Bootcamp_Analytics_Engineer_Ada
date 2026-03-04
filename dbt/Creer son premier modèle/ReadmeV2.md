# Chapitre 2 — Créer son premier modèle dbt

---

## Table des matières

1. [Rappel : Introduction à Jinja](#1-rappel--introduction-à-jinja)
2. [Introduction aux modèles dbt](#2-introduction-aux-modèles-dbt)
3. [Introduction aux CTE](#3-introduction-aux-cte)
4. [Création des premiers modèles](#4-création-des-premiers-modèles)
   - 4.1 [Préparation de l'environnement dbt Cloud](#41-préparation-de-lenvironnement-dbt-cloud)
   - 4.2 [Le concept de curation](#42-le-concept-de-curation)
   - 4.3 [Modèle 1 : `curation_hosts`](#43-modèle-1--curation_hosts)
   - 4.4 [Modèle 2 : `curation_listings`](#44-modèle-2--curation_listings)
   - 4.5 [Exercice : Modèle 3 — `curation_reviews`](#45-exercice--modèle-3--curation_reviews)
5. [Points clés à retenir](#5-points-clés-à-retenir)

---

## Sommaire du chapitre

Ce chapitre couvre trois grandes parties :

1. **Introduction aux modèles** — Qu'est-ce qu'un modèle dbt et quelle est sa place dans le projet
2. **Introduction aux CTE** — La syntaxe SQL essentielle utilisée dans les modèles dbt
3. **Création de nos premiers modèles** — Mise en pratique avec 3 modèles concrets (hosts, listings, reviews)

---

## 1. Rappel : Introduction à Jinja

Jinja est un **moteur de template** utilisé par le **langage Python**. Créé et distribué sous licence BSD, il **fournit des expressions Python** et évalue les **templates** dans une sandbox. C'est un **langage orienté texte** qui peut ainsi être utilisé pour **générer** n'importe quel type de fichier pouvant être **balisé**.

> **Lien avec dbt** : dbt utilise Jinja pour rendre le SQL dynamique. Chaque fichier `.sql` d'un projet dbt est en réalité un template Jinja qui sera compilé avant d'être exécuté dans le warehouse. Pour le détail de la syntaxe Jinja (boucles, conditions, macros), se référer au notebook joint et au chapitre précédent.

---

## 2. Introduction aux modèles dbt

### Définition

Les modèles sont les **ingrédients essentiels** de dbt. Ce sont des fichiers qui contiennent des requêtes SQL et qui servent à définir des **tables**, des **vues**, ou de simples parties d'une requête plus large.

Caractéristiques des modèles dbt :

- Ils permettent de définir les tables, vues, etc. dans le warehouse
- Ils sont stockés dans des fichiers `.sql` sous le dossier **`models/`**
- Les modèles peuvent **se référencer entre eux**, utiliser des templates et des macros Jinja

> **En résumé** : un modèle dbt = un fichier `.sql` dans le dossier `models/` qui contient une requête SELECT. dbt se charge de créer la table ou la vue correspondante dans le warehouse.

---

## 3. Introduction aux CTE

### Qu'est-ce qu'une CTE ?

Une **CTE** (Common Table Expression) est une sous-requête nommée et temporaire, définie à l'aide du mot-clé `WITH`. Elle permet de structurer des requêtes complexes en étapes lisibles et réutilisables. Les CTE sont le **pattern fondamental** utilisé dans les modèles dbt.

### Syntaxe générale

```sql
WITH
    expression_1 AS (
        SELECT ...
    ),
    expression_2 AS (
        SELECT ...
    ),
    expression_3 AS (
        SELECT ...
        FROM expression_1    -- Une CTE peut référencer une CTE précédente
    )
SELECT ...
FROM expression_2
JOIN expression_3
ON ...
```

**Décryptage de la structure :**

1. **`WITH`** — Mot-clé qui ouvre le bloc de CTE. Il n'apparaît qu'une seule fois, tout au début de la requête.
2. **`expression_1 AS (...)`** — Première CTE : on lui donne un nom et on définit sa requête entre parenthèses. C'est comme une "table virtuelle temporaire".
3. **La virgule `,`** — Sépare les CTE entre elles. Attention : pas de virgule après la dernière CTE.
4. **`expression_3`** réutilise **`expression_1`** — Une CTE peut faire référence à une CTE définie avant elle (mais pas après).
5. **Le `SELECT` final** — C'est la requête principale qui utilise les CTE comme des tables. Elle peut faire des `JOIN`, des `WHERE`, des `GROUP BY`, etc.

### Exemple concret avec les données Airbnb

```sql
WITH listings_raw AS (
    SELECT
        id,
        listing_url,
        name,
        description,
        neighbourhood_overview,
        host_id,
        latitude,
        longitude,
        property_type,
        room_type,
        accommodates,
        bathrooms,
        bedrooms,
        beds,
        amenities,
        price,
        minimum_nights,
        maximum_nights
    FROM airbnb.raw.listings
)
SELECT
    *
FROM listings_raw
WHERE minimum_nights > 0
```

Dans cet exemple, la CTE `listings_raw` sélectionne toutes les colonnes de la table brute, puis la requête principale filtre pour ne garder que les listings avec un minimum de nuits supérieur à zéro. Cette structure en deux temps (CTE de sélection + requête finale de transformation/filtrage) est le pattern standard dans dbt.

---

## 4. Création des premiers modèles

### 4.1 Préparation de l'environnement dbt Cloud

Avant de créer vos modèles, quelques étapes de préparation sont nécessaires dans l'IDE dbt Cloud.

**Vérification du rôle Snowflake :**

> **⚠️ Important** : assurez-vous que le rôle Snowflake configuré dans dbt Cloud est bien `transform`. Pour vérifier : cliquez sur votre nom en bas à gauche de l'écran → Profile → Connection → dans la section « Optional settings », vérifiez que le rôle est bien `transform`. Si ce n'est pas le cas, cliquez sur le bouton **Edit** en haut à droite pour le modifier.

**Création d'une branche de travail :**

Dans l'IDE dbt Cloud, créez une nouvelle branche pour travailler sans impacter la branche principale. Cliquez sur **"Create branch"** et nommez-la `curation`, puis cliquez sur **Submit**.

**Nettoyage du projet :**

Supprimez le dossier `example` dans `models/` (clic droit → Delete). Ce dossier contient les modèles d'exemple générés automatiquement (`my_first_dbt_model.sql`, `my_second_dbt_model.sql`, `schema.yml`) et ne sera plus utile.

**Création du dossier de curation :**

Clic droit sur le dossier `models/` → **Create folder** → nommez-le `curation` → cliquez sur **Create**.

La structure du projet devient :

```
models/
└── curation/        ← notre nouveau dossier
```

---

### 4.2 Le concept de curation

L'idée de cette étape est de passer des données **brutes** (RAW) à des données **curées** (nettoyées et transformées). On crée un modèle de curation pour chacune de nos 3 tables sources :

```
┌─────────────┐           ┌─────────────────┐
│     RAW     │           │    CURATION     │
├─────────────┤           ├─────────────────┤
│  Listings   │  ──────►  │  Listings       │
│  Hosts      │  ──────►  │  Hosts          │
│  Reviews    │  ──────►  │  Reviews        │
└─────────────┘           └─────────────────┘
```

Chaque modèle de curation applique des transformations simples : renommage de colonnes, conversion de types, nettoyage de valeurs, création de colonnes dérivées.

---

### 4.3 Modèle 1 : `curation_hosts`

#### Étape 1 — Tester la requête dans Snowflake

Avant de créer le modèle dbt, il est recommandé de d'abord écrire et tester votre SQL directement dans Snowflake. Voici la requête de curation pour la table `hosts` :

```sql
WITH hosts_raw AS (
    SELECT
        host_id,
        CASE
            WHEN LEN(host_name) = 1 THEN 'Anonyme'
            ELSE host_name
        END AS host_name,
        host_since,
        host_location,
        SPLIT_PART(host_location, ',', 1) AS host_city,
        SPLIT_PART(host_location, ',', 2) AS host_country,
        TRY_CAST(REPLACE(host_response_rate, '%', '') AS INTEGER) AS response_rate,
        host_is_superhost = 't' AS is_superhost,
        host_neighbourhood,
        host_identity_verified = 't' AS is_identity_verified
    FROM airbnb.raw.hosts
)
SELECT *
FROM hosts_raw
```

**Décryptage des transformations ligne par ligne :**

1. **`host_id`** — Conservé tel quel, c'est la clé primaire.

2. **`CASE WHEN LEN(host_name) = 1 THEN 'Anonyme' ELSE host_name END AS host_name`** — Si le nom de l'hôte ne contient qu'un seul caractère (probablement une initiale, donc inutilisable), on le remplace par `'Anonyme'`. Sinon, on garde le nom original.

3. **`host_since`** — Date conservée telle quelle.

4. **`host_location`** — Valeur brute conservée pour référence (ex : `"Amsterdam, Netherlands"`).

5. **`SPLIT_PART(host_location, ',', 1) AS host_city`** — La fonction `SPLIT_PART` découpe la chaîne `host_location` sur le séparateur `,` et prend la **1ère partie** (la ville). Exemple : `"Amsterdam, Netherlands"` → `"Amsterdam"`.

6. **`SPLIT_PART(host_location, ',', 2) AS host_country`** — Même principe, mais on prend la **2ème partie** (le pays). Exemple : `"Amsterdam, Netherlands"` → `" Netherlands"`.

7. **`TRY_CAST(REPLACE(host_response_rate, '%', '') AS INTEGER) AS response_rate`** — Transformation en deux temps : d'abord `REPLACE` supprime le caractère `%` de la chaîne (ex : `"100%"` → `"100"`), puis `TRY_CAST` convertit le résultat en entier. On utilise `TRY_CAST` plutôt que `CAST` car il retourne `NULL` au lieu de lever une erreur si la conversion échoue.

8. **`host_is_superhost = 't' AS is_superhost`** — Compare la valeur à `'t'` (true). Le résultat est un **booléen** : `TRUE` si l'hôte est superhost, `FALSE` sinon. On renomme aussi la colonne en `is_superhost` (plus lisible).

9. **`host_neighbourhood`** — Conservé tel quel.

10. **`host_identity_verified = 't' AS is_identity_verified`** — Même logique booléenne que pour le superhost.

**Résultat attendu dans Snowflake** : 7,8K lignes avec les nouvelles colonnes `HOST_CITY`, `HOST_COUNTRY`, `RESPONSE_RATE` (en integer), `IS_SUPERHOST` (booléen) et `IS_IDENTITY_VERIFIED` (booléen).

#### Étape 2 — Créer le modèle dans dbt Cloud

1. Dans l'explorateur de fichiers, clic droit sur le dossier `curation/` → **Create file**
2. Nommez le fichier : **`curation_hosts.sql`**

> **⚠️ Le fichier doit impérativement avoir l'extension `.sql`** — c'est ce qui permet à dbt de le reconnaître comme un modèle.

3. Collez la requête SQL dans le fichier

> **⚠️ Pas de point-virgule `;` à la fin !** Contrairement au SQL classique, les modèles dbt ne prennent **pas** de `;` final. Chaque fichier `.sql` contient **une seule requête SELECT** et dbt gère lui-même la terminaison. Si vous laissez un `;`, le build échouera.

4. Cliquez sur **Save** (en haut à droite)
5. Cliquez sur **Build** (dans la barre en bas)

La commande exécutée sera : `dbt build --select curation_hosts`

#### Étape 3 — Vérifier le résultat

**Dans dbt Cloud :**

- L'onglet **Lineage** montre un nœud unique `curation_hosts` (MDL = model)
- Les logs affichent : `1 of 1 OK created sql view model RAW.curation_hosts [SUCCESS 1 in 0.91s]`

**Dans Snowflake :**

Vérifiez en naviguant dans la base de données : `AIRBNB` → `RAW` → **Views** → `CURATION_HOSTS`.

La vue contient 10 colonnes : `HOST_ID`, `HOST_NAME`, `HOST_SINCE`, `HOST_LOCATION`, `HOST_CITY`, `HOST_COUNTRY`, `RESPONSE_RATE`, `IS_SUPERHOST`, `HOST_NEIGHBOURHOOD`, `IS_IDENTITY_VERIFIED`.

Vous pouvez interroger cette vue directement dans Snowflake :

```sql
SELECT * FROM AIRBNB.RAW.CURATION_HOSTS LIMIT 10;
```

> **Observation importante** : dbt crée par défaut des **vues** (et non des tables). C'est la matérialisation par défaut. On pourra changer ce comportement plus tard dans le cours.

> **Note sur le schéma** : vous remarquerez que la vue est créée dans le schéma `RAW` (et non dans un schéma séparé « curation »). C'est parce que le schéma par défaut configuré dans les credentials dbt est `RAW`. Ce point sera corrigé dans un chapitre ultérieur sur l'organisation des schémas.

---

### 4.4 Modèle 2 : `curation_listings`

On répète le même procédé : écrire le SQL dans Snowflake, puis le copier dans un nouveau fichier `.sql` dans dbt.

#### Étape 1 — Créer le fichier

Clic droit sur `curation/` → **Create file** → nommez-le **`curation_listings.sql`**.

#### Étape 2 — Le code SQL

```sql
WITH listings_raw AS
(SELECT
        id AS listing_id,
        listing_url,
        name,
        description,
        description IS NOT NULL AS has_description,
        neighbourhood_overview,
        neighbourhood_overview IS NOT NULL AS has_neighbourhood_description,
        host_id,
        latitude,
        longitude,
        property_type,
        room_type,
        accommodates,
        bathrooms,
        bedrooms,
        beds,
        amenities,
        TRY_CAST(SPLIT_PART(price, '$', 1) AS FLOAT) AS price,
        minimum_nights,
        maximum_nights
    FROM airbnb.raw.listings)
SELECT *
FROM listings_raw
```

**Décryptage des transformations :**

1. **`id AS listing_id`** — Renommage de `id` en `listing_id` pour plus de clarté. Dans un projet avec beaucoup de tables, avoir des identifiants préfixés évite toute ambiguïté dans les jointures.

2. **`description IS NOT NULL AS has_description`** — Crée un booléen qui indique si le listing possède une description. Utile pour des analyses de qualité de données.

3. **`neighbourhood_overview IS NOT NULL AS has_neighbourhood_description`** — Même logique pour la description du quartier.

4. **`TRY_CAST(SPLIT_PART(price, '$', 1) AS FLOAT) AS price`** — Transformation en deux temps : `SPLIT_PART` supprime le symbole `$` du prix (ex : `"$950.00"` → `"950.00"`), puis `TRY_CAST` convertit en nombre décimal. On peut ainsi faire des calculs sur les prix.

5. Toutes les autres colonnes sont conservées telles quelles.

#### Étape 3 — Save + Build

Sauvegardez le fichier et cliquez sur **Build**. La commande exécutée est : `dbt build --select curation_listings`.

**Résultat attendu** : `dbt build succeeded` — la vue `CURATION_LISTINGS` est créée dans Snowflake sous `AIRBNB` → `RAW` → **Views**.

**Vérification dans Snowflake** : vous pouvez voir les deux vues côte à côte : `CURATION_HOSTS` et `CURATION_LISTINGS`.

---

### 4.5 Exercice : Modèle 3 — `curation_reviews`

À vous de jouer ! Créez un troisième modèle pour la table `reviews` en suivant le même procédé.

**Consignes :**

1. Créer un fichier `curation_reviews.sql` dans le dossier `models/curation/`
2. Appliquer les transformations suivantes :
   - Renommer la colonne `date` en **`review_date`** (plus explicite)
   - Ajouter une colonne **`number_reviews`** qui contient le nombre de reviews que chaque listing a reçu **pour chaque jour** (c'est-à-dire : compter le nombre de reviews par `listing_id` et par `date`)

**Indice** : vous aurez besoin d'un `GROUP BY` et de la fonction d'agrégation `COUNT(*)`.

**Solution :**

<details>
<summary>Cliquez pour révéler la solution</summary>

```sql
WITH reviews_raw AS (
    SELECT
        listing_id,
        date AS review_date
    FROM airbnb.raw.reviews
)
SELECT
    listing_id,
    review_date,
    COUNT(*) AS number_reviews
FROM reviews_raw
GROUP BY listing_id, review_date
```

**Explication :**

1. La CTE `reviews_raw` sélectionne les données brutes et renomme `date` en `review_date`.
2. La requête principale agrège les données par `listing_id` et `review_date`, et compte le nombre de lignes pour chaque combinaison. Ainsi, si le listing 262394 a reçu 2 commentaires le 11 avril 2012, la colonne `number_reviews` affichera `2` pour cette ligne.

</details>

> **Rappel** : pas de `;` à la fin du fichier ! Sauvegardez, puis cliquez sur **Build**.

---

## 5. Points clés à retenir

### Les règles d'or des modèles dbt

1. **Un modèle = un fichier `.sql` = une requête SELECT** — Chaque modèle contient une seule requête. Pas de `CREATE TABLE`, pas de `INSERT INTO` : dbt gère tout cela automatiquement.

2. **Pas de point-virgule `;`** — Contrairement au SQL pur, les modèles dbt ne prennent pas de `;` en fin de requête. Si vous en mettez un, le build échouera.

3. **Extension `.sql` obligatoire** — Le fichier doit impérativement avoir l'extension `.sql` pour être reconnu comme un modèle.

4. **dbt crée des vues par défaut** — La matérialisation par défaut est `view`. C'est un choix qui sera modifiable plus tard (table, incremental, ephemeral).

5. **Le workflow est toujours le même** : écrire et tester le SQL dans Snowflake → copier dans un fichier `.sql` dans le dossier `models/` → **Save** → **Build**.

### Commandes dbt utilisées dans ce chapitre

| Commande | Description |
|----------|-------------|
| `dbt build --select curation_hosts` | Compile et exécute uniquement le modèle `curation_hosts` |
| `dbt build --select curation_listings` | Compile et exécute uniquement le modèle `curation_listings` |
| `dbt build --select curation_reviews` | Compile et exécute uniquement le modèle `curation_reviews` |

### Fonctions Snowflake découvertes

| Fonction | Rôle | Exemple |
|----------|------|---------|
| `SPLIT_PART(str, sep, n)` | Découpe une chaîne et retourne la n-ième partie | `SPLIT_PART('Amsterdam, NL', ',', 1)` → `'Amsterdam'` |
| `REPLACE(str, old, new)` | Remplace toutes les occurrences d'une sous-chaîne | `REPLACE('100%', '%', '')` → `'100'` |
| `TRY_CAST(expr AS type)` | Convertit le type, retourne `NULL` en cas d'échec (au lieu d'erreur) | `TRY_CAST('100' AS INTEGER)` → `100` |
| `LEN(str)` | Retourne la longueur d'une chaîne | `LEN('A')` → `1` |

### Structure du projet à ce stade

```
models/
└── curation/
    ├── curation_hosts.sql        ← Vue CURATION_HOSTS dans Snowflake
    ├── curation_listings.sql     ← Vue CURATION_LISTINGS dans Snowflake
    └── curation_reviews.sql      ← Vue CURATION_REVIEWS dans Snowflake
```

Dans Snowflake, ces modèles apparaissent sous `AIRBNB` → `RAW` → **Views** :

```
AIRBNB.RAW
├── Tables
│   ├── HOSTS
│   ├── LISTINGS
│   └── REVIEWS
└── Views
    ├── CURATION_HOSTS        ← créée par dbt
    ├── CURATION_LISTINGS     ← créée par dbt
    └── CURATION_REVIEWS      ← créée par dbt
```

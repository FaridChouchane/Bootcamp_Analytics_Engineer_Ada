# Projet final chapitre DBT 

## objectif : 

Produire 4 fichiers SQL d’analyse (pas des modèles de transformation) en utilisant les tables déjà créées dans le pipeline.


### 1️⃣ Distribution des prix par quartier à Amsterdam

Objectif : comprendre comment les prix sont répartis selon les quartiers.

Exemple de fichier :
analyses/price_distribution_by_neighbourhood.sql






### 2️⃣ Distribution des superhosts par quartier

Objectif : voir où sont concentrés les superhosts.

Fichier :
analyses/superhosts_distribution.sql


### 3️⃣ Relation entre statut superhost et prix

Question métier :
👉 Les superhosts sont-ils plus chers ?

Fichier :
analyses/superhost_price_relationship.sql

### 4️⃣ Airbnb vs hôtels (évolution dans le temps)

Tu dois utiliser :

curation_tourists_per_year

les reviews Airbnb par an

Idée :

plus de reviews Airbnb → plus d'utilisation Airbnb

comparer avec le nombre de touristes.
Fichier :
analyses/airbnb_vs_tourism_trend.sql

### 📂 Structure finale :

analyses/</br>
│</br>
├── price_distribution_by_neighbourhood.sql</br>
├── superhosts_distribution.sql</br>
├── superhost_price_relationship.sql</br>
└── airbnb_vs_tourism_trend.sql</br>

### ▶️ Exécution
```Bash
dbt compile
```

### ⭐Ajouts:

- median_price
- percentiles
- ratio superhost
- ratio airbnb / touristes

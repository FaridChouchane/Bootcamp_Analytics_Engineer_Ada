# 🏥 Projet Final Snowflake – HEALTH_APP Pipeline ETL

## Description

Pipeline ETL complet pour l'application santé **HEALTH_APP**.  
Il ingère des événements de logs, les valide, les enrichit, et gère les anomalies et les données tardives via un graphe de TASKs automatisées.

---

## Architecture

```
HEALTH_APP
├── RAW      → RAW_EVENTS (entrée brute), RAW_EVENTS_STREAM, UDFs, Procédures, TASKs
├── STAGING  → 20 tables par process_name (HiH_*, Step_*)
└── CONTROL  → DATA_ANOMALIES, TASK_LOGS  ⬅ schéma non-métier dédié
```

### Règles de traitement

| Cas | Condition | Destination |
|-----|-----------|-------------|
| ✅ Valide | timestamp OK + process_name dans whitelist + ≤ 5 jours | `STAGING.<process_name>` |
| ❌ Invalide | timestamp hors plage ou process_name inconnu/null | `CONTROL.DATA_ANOMALIES` (type = INVALID) |
| ⏰ Tardif | `event_timestamp` > 5 jours par rapport à aujourd'hui | `CONTROL.DATA_ANOMALIES` (type = LATE) |

> Les données **tardives ne sont jamais chargées dans STAGING**, même si elles sont techniquement valides.

---

## Modèle RBAC

| Rôle | Droits |
|------|--------|
| `ADMIN_ROLE` | Tous droits — hérite de APP_ROLE et ROLE_ENGINEER |
| `APP_ROLE` | Crée et exécute les TASKs, lit/écrit toutes les tables |
| `ROLE_ENGINEER` | Lecture seule sur RAW, STAGING, CONTROL + monitoring TASKs |

> Les TASKs sont créées avec `APP_ROLE` conformément aux consignes.

---

## Graphe de TASKs

```
DATA_QUALITY_TASK  ← planifiée toutes les heures
│   Appelle raw.data_quality()
│   → invalides + tardives  →  CONTROL.DATA_ANOMALIES
│
├── ENRICH_DATA_HIH_*
├── ENRICH_DATA_HIH_LISTENER_MANAGER
├── ENRICH_DATA_HIH_HIBROADCASTUTIL
├── ENRICH_DATA_HIH_DATASTATMANAGER
├── ENRICH_DATA_HIH_HIAPPUTIL
├── ENRICH_DATA_HIH_HIHEALTHBINDER
├── ENRICH_DATA_HIH_HIHEALTHDATAINSERTSTORE
├── ENRICH_DATA_HIH_HISYNCCONTROL
├── ENRICH_DATA_HIH_HISYNCUTIL
├── ENRICH_DATA_STEP_DATACACHE
├── ENRICH_DATA_STEP_EXTSDM
├── ENRICH_DATA_STEP_FLUSHABLESTEPDATACACHE
├── ENRICH_DATA_STEP_HGNH
├── ENRICH_DATA_STEP_LSC
├── ENRICH_DATA_STEP_NOTIFICATIONUTIL
├── ENRICH_DATA_STEP_SPUTILS
├── ENRICH_DATA_STEP_SCREENUTIL
├── ENRICH_DATA_STEP_STANDREPORTRECEIVER
├── ENRICH_DATA_STEP_STANDSTEPCOUNTER
└── ENRICH_DATA_STEP_STANDSTEPDATAMANAGER
    Chacune appelle raw.enrich_data(task_key, process_name)
    → valides + non-tardifs  →  STAGING.<process_name>
```

---

## Structure des fichiers

```
health_projet_final/
├── scripts/
│   ├── 01_setup.sql                 ← DB, schémas, warehouse
│   ├── 02_rbac.sql                  ← Rôles et permissions
│   ├── 03_tables.sql                ← Tables RAW, STAGING (×20), CONTROL + stream
│   ├── 04_functions_procedures.sql  ← UDFs + procédures data_quality & enrich_data
│   ├── 05_tasks.sql                 ← 21 TASKs (1 racine + 20 enfants)
│   └── 06_test_data.sql             ← Données de test (3 cas : valide / invalide / tardif)
├── deploy.sql                       ← Script principal (appelle tout dans l'ordre)
└── README.md
```

---

## Déploiement

### Prérequis

- Compte Snowflake avec rôle `ACCOUNTADMIN` disponible
- [SnowSQL](https://docs.snowflake.com/en/user-guide/snowsql.html) installé — vérifier avec `snowsql --version`
- Warehouse `COMPUTE_WH` existant (conservé pour compatibilité)

### Déploiement complet via SnowSQL

```bash
# 1. Cloner le dépôt
git clone https://github.com/<votre-username>/health_projet_final.git
cd health_projet_final

# 2. Déployer tout en une commande
snowsql -a <account_identifier> -u <username> -f deploy.sql
```

> Remplacer `<account_identifier>` par ex. `xy12345.west-europe.azure`  
> Remplacer `<username>` par votre identifiant Snowflake

### Déploiement script par script

```bash
snowsql -a <account> -u <user> -f scripts/01_setup.sql
snowsql -a <account> -u <user> -f scripts/02_rbac.sql
snowsql -a <account> -u <user> -f scripts/03_tables.sql
snowsql -a <account> -u <user> -f scripts/04_functions_procedures.sql
snowsql -a <account> -u <user> -f scripts/05_tasks.sql
```

---

## Tests

### 1. Injecter les données de test

```bash
snowsql -a <account> -u <user> -f scripts/06_test_data.sql
```

Les données insérées couvrent les **3 cas** :

| # | Cas | Détail |
|---|-----|--------|
| 1 | ✅ Valide | 4 events — timestamp récent + process_name valide |
| 2 | ❌ Invalide | 4 events — timestamp NULL, timestamp <2016, process inconnu, process NULL |
| 3 | ⏰ Tardif | 3 events — -6j, -10j, -30j par rapport à aujourd'hui |

### 2. Déclencher le graphe manuellement

```sql
USE ROLE APP_ROLE;
EXECUTE TASK HEALTH_APP.RAW.DATA_QUALITY_TASK;
-- Attendre ~30 secondes que les TASKs enfants s'exécutent
```

### 3. Vérifications (avec ROLE_ENGINEER)

```sql
USE ROLE ROLE_ENGINEER;
USE DATABASE HEALTH_APP;
USE WAREHOUSE PIPELINE_WH;
```

#### ✅ Cas valide — lignes présentes dans STAGING

```sql
SELECT * FROM HEALTH_APP.STAGING."HiH_ListenerManager";   -- 1 ligne
SELECT * FROM HEALTH_APP.STAGING."Step_LSC";              -- 1 ligne
SELECT * FROM HEALTH_APP.STAGING."Step_StandStepCounter"; -- 1 ligne
SELECT * FROM HEALTH_APP.STAGING."HiH_HiSyncUtil";        -- 1 ligne
```

#### ❌ Cas invalide — 4 anomalies type INVALID

```sql
SELECT anomaly_id, event_id, process_name, event_timestamp, anomaly_type, reason
FROM HEALTH_APP.CONTROL.DATA_ANOMALIES
WHERE anomaly_type = 'INVALID'
ORDER BY detected_at DESC;
-- Attendu : 4 lignes
```

#### ⏰ Cas tardif — 3 anomalies type LATE

```sql
SELECT anomaly_id, event_id, process_name, event_timestamp, anomaly_type, reason
FROM HEALTH_APP.CONTROL.DATA_ANOMALIES
WHERE anomaly_type = 'LATE'
ORDER BY detected_at DESC;
-- Attendu : 3 lignes (-6j, -10j, -30j)
```

#### 🔒 Vérifier que les tardifs ne sont PAS dans STAGING

```sql
SELECT * FROM HEALTH_APP.STAGING."Step_HGNH"
WHERE process_id IN (1020, 1021, 1022);
-- Attendu : 0 lignes
```

#### 📋 Logs d'exécution des TASKs

```sql
SELECT * FROM HEALTH_APP.CONTROL.TASK_LOGS
ORDER BY execution_time DESC
LIMIT 30;
```

#### 📊 Historique Snowflake des TASKs

```sql
SELECT *
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
))
ORDER BY SCHEDULED_TIME DESC;
```

#### 🔐 Tester les restrictions ROLE_ENGINEER (doit échouer)

```sql
USE ROLE ROLE_ENGINEER;
INSERT INTO HEALTH_APP.CONTROL.DATA_ANOMALIES (event_id) VALUES (9999);
-- Erreur attendue : "Insufficient privileges to operate on table"
```

---

## Ce qui a été refactorisé par rapport au pipeline d'origine

| Avant | Après |
|-------|-------|
| `RAW.wrong_datas` et `RAW.data_anomalies` dans le schéma RAW | `CONTROL.DATA_ANOMALIES` dans un schéma dédié non-métier |
| Pas de gestion des données tardives | Détection dans `data_quality()` + colonne `anomaly_type = 'LATE'` |
| `ACCOUNTADMIN` pour tout | Modèle RBAC : ADMIN_ROLE / APP_ROLE / ROLE_ENGINEER |
| TASKs créées en ACCOUNTADMIN | TASKs créées et exécutées avec APP_ROLE |
| `enrich_data()` insère sans filtres | Filtre strict : valides ET non-tardives uniquement |
| Pas de stream | `RAW_EVENTS_STREAM` (APPEND_ONLY) — ne traite que les nouvelles lignes |
| 19 procédures `enrich_data` séparées | 1 seule procédure dynamique paramétrée |

---

## Pousser sur GitHub

```bash
git init
git add .
git commit -m "feat: projet final Snowflake HEALTH_APP pipeline ETL"
git branch -M main
git remote add origin https://github.com/<votre-username>/health_projet_final.git
git push -u origin main
```

---

## Scripts SQL complets

### 📄 scripts/01_setup.sql

```sql
-- =============================================================
-- 01_setup.sql
-- Base de données, schémas, warehouse
-- Rôle requis : ACCOUNTADMIN
-- =============================================================

USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS HEALTH_APP
    COMMENT = 'Base principale application santé';

CREATE SCHEMA IF NOT EXISTS HEALTH_APP.RAW
    COMMENT = 'Données brutes avant transformation — ne jamais modifier directement';

CREATE SCHEMA IF NOT EXISTS HEALTH_APP.STAGING
    COMMENT = 'Données enrichies et validées par process';

-- Schéma dédié tout ce qui n'est PAS strictement métier
CREATE SCHEMA IF NOT EXISTS HEALTH_APP.CONTROL
    COMMENT = 'Anomalies, logs, contrôles qualité — schéma non-métier';

CREATE WAREHOUSE IF NOT EXISTS PIPELINE_WH
    WAREHOUSE_SIZE      = 'X-SMALL'
    AUTO_SUSPEND        = 60
    AUTO_RESUME         = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT             = 'Warehouse dédié pipeline HEALTH_APP';
```

---

### 📄 scripts/02_rbac.sql

```sql
-- =============================================================
-- 02_rbac.sql
-- Modèle RBAC complet
-- Rôle requis : ACCOUNTADMIN
-- =============================================================

USE ROLE ACCOUNTADMIN;
USE DATABASE HEALTH_APP;

-- 1. CRÉATION DES RÔLES
CREATE ROLE IF NOT EXISTS ADMIN_ROLE
    COMMENT = 'Administration complète du pipeline HEALTH_APP';

CREATE ROLE IF NOT EXISTS APP_ROLE
    COMMENT = 'Rôle applicatif — crée et exécute les TASKs';

CREATE ROLE IF NOT EXISTS ROLE_ENGINEER
    COMMENT = 'Lecture seule — vérification du pipeline';

-- 2. HIÉRARCHIE
GRANT ROLE APP_ROLE      TO ROLE ADMIN_ROLE;
GRANT ROLE ROLE_ENGINEER TO ROLE ADMIN_ROLE;
GRANT ROLE ADMIN_ROLE    TO ROLE SYSADMIN;

-- 3. WAREHOUSE
GRANT USAGE ON WAREHOUSE PIPELINE_WH TO ROLE APP_ROLE;
GRANT USAGE ON WAREHOUSE PIPELINE_WH TO ROLE ROLE_ENGINEER;
GRANT USAGE ON WAREHOUSE COMPUTE_WH  TO ROLE APP_ROLE;
GRANT USAGE ON WAREHOUSE COMPUTE_WH  TO ROLE ROLE_ENGINEER;

-- 4. BASE DE DONNÉES
GRANT USAGE ON DATABASE HEALTH_APP TO ROLE APP_ROLE;
GRANT USAGE ON DATABASE HEALTH_APP TO ROLE ROLE_ENGINEER;

-- 5. SCHÉMAS — APP_ROLE
GRANT USAGE, CREATE TABLE, CREATE TASK, CREATE STREAM, CREATE PROCEDURE, CREATE FUNCTION
    ON SCHEMA HEALTH_APP.RAW     TO ROLE APP_ROLE;
GRANT USAGE, CREATE TABLE ON SCHEMA HEALTH_APP.STAGING TO ROLE APP_ROLE;
GRANT USAGE, CREATE TABLE ON SCHEMA HEALTH_APP.CONTROL TO ROLE APP_ROLE;

GRANT ALL PRIVILEGES ON ALL TABLES     IN SCHEMA HEALTH_APP.RAW     TO ROLE APP_ROLE;
GRANT ALL PRIVILEGES ON ALL TABLES     IN SCHEMA HEALTH_APP.STAGING TO ROLE APP_ROLE;
GRANT ALL PRIVILEGES ON ALL TABLES     IN SCHEMA HEALTH_APP.CONTROL TO ROLE APP_ROLE;
GRANT USAGE ON ALL PROCEDURES          IN SCHEMA HEALTH_APP.RAW     TO ROLE APP_ROLE;
GRANT USAGE ON ALL FUNCTIONS           IN SCHEMA HEALTH_APP.RAW     TO ROLE APP_ROLE;

GRANT ALL PRIVILEGES ON FUTURE TABLES     IN SCHEMA HEALTH_APP.RAW     TO ROLE APP_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES     IN SCHEMA HEALTH_APP.STAGING TO ROLE APP_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES     IN SCHEMA HEALTH_APP.CONTROL TO ROLE APP_ROLE;
GRANT USAGE ON FUTURE PROCEDURES IN SCHEMA HEALTH_APP.RAW TO ROLE APP_ROLE;
GRANT USAGE ON FUTURE FUNCTIONS  IN SCHEMA HEALTH_APP.RAW TO ROLE APP_ROLE;

GRANT EXECUTE TASK ON ACCOUNT TO ROLE APP_ROLE;

-- 6. SCHÉMAS — ROLE_ENGINEER (lecture seule)
GRANT USAGE ON SCHEMA HEALTH_APP.RAW     TO ROLE ROLE_ENGINEER;
GRANT USAGE ON SCHEMA HEALTH_APP.STAGING TO ROLE ROLE_ENGINEER;
GRANT USAGE ON SCHEMA HEALTH_APP.CONTROL TO ROLE ROLE_ENGINEER;

GRANT SELECT ON ALL TABLES    IN SCHEMA HEALTH_APP.RAW     TO ROLE ROLE_ENGINEER;
GRANT SELECT ON ALL TABLES    IN SCHEMA HEALTH_APP.STAGING TO ROLE ROLE_ENGINEER;
GRANT SELECT ON ALL TABLES    IN SCHEMA HEALTH_APP.CONTROL TO ROLE ROLE_ENGINEER;

GRANT SELECT ON FUTURE TABLES IN SCHEMA HEALTH_APP.RAW     TO ROLE ROLE_ENGINEER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA HEALTH_APP.STAGING TO ROLE ROLE_ENGINEER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA HEALTH_APP.CONTROL TO ROLE ROLE_ENGINEER;

GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE ROLE_ENGINEER;
```

---

### 📄 scripts/03_tables.sql

```sql
-- =============================================================
-- 03_tables.sql
-- Tables RAW + STAGING (×20) + CONTROL + Stream
-- Rôle requis : APP_ROLE
-- =============================================================

USE ROLE APP_ROLE;
USE DATABASE HEALTH_APP;
USE WAREHOUSE PIPELINE_WH;

-- RAW.RAW_EVENTS
CREATE TABLE IF NOT EXISTS HEALTH_APP.RAW.RAW_EVENTS (
    event_id        NUMBER AUTOINCREMENT PRIMARY KEY,
    event_timestamp TIMESTAMP_NTZ,
    process_name    STRING,
    process_id      NUMBER,
    message         STRING,
    loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Stream APPEND_ONLY — capture uniquement les nouvelles lignes
CREATE STREAM IF NOT EXISTS HEALTH_APP.RAW.RAW_EVENTS_STREAM
    ON TABLE HEALTH_APP.RAW.RAW_EVENTS
    APPEND_ONLY = TRUE;

-- STAGING — 20 tables par process_name
CREATE TABLE IF NOT EXISTS HEALTH_APP.STAGING."HiH_"                        (event_timestamp TIMESTAMP_NTZ, log_trigger STRING, process_id NUMBER, message STRING);
CREATE TABLE IF NOT EXISTS HEALTH_APP.STAGING."HiH_DataStatManager"          (event_timestamp TIMESTAMP_NTZ, log_trigger STRING, process_id NUMBER, message STRING);
CREATE TABLE IF NOT EXISTS HEALTH_APP.STAGING."HiH_HiAppUtil"                (event_timestamp TIMESTAMP_NTZ, log_trigger STRING, process_id NUMBER, message STRING);
CREATE TABLE IF NOT EXISTS HEALTH_APP.STAGING."HiH_HiBroadcastUtil"          (event_timestamp TIMESTAMP_NTZ, log_trigger STRING, process_id NUMBER, message STRING);
CREATE TABLE IF NOT EXISTS HEALTH_APP.STAGING."HiH_HiHealthBinder"           (event_timestamp TIMESTAMP_NTZ, log_trigger STRING, process_id NUMBER, message STRING);
CREATE TABLE IF NOT EXISTS HEALTH_APP.STAGING."HiH_HiHealthDataInsertStore"  (event_timestamp TIMESTAMP_NTZ, log_trigger STRING, process_id NUMBER, message STRING);
CREATE TABLE IF NOT EXISTS HEALTH_APP.STAGING."HiH_HiSyncControl"            (event_timestamp TIMESTAMP_NTZ, log_trigger STRING, process_id NUMBER, message STRING);
CREATE TABLE IF NOT EXISTS HEALTH_APP.STAGING."HiH_HiSyncUtil"               (event_timestamp TIMESTAMP_NTZ, log_trigger STRING, process_id NUMBER, message STRING);
CREATE TABLE IF NOT EXISTS HEALTH_APP.STAGING."HiH_ListenerManager"          (event_timestamp TIMESTAMP_NTZ, log_trigger STRING, process_id NUMBER, message STRING);
CREATE TABLE IF NOT EXISTS HEALTH_APP.STAGING."Step_DataCache"               (event_timestamp TIMESTAMP_NTZ, log_trigger STRING, process_id NUMBER, message STRING);
CREATE TABLE IF NOT EXISTS HEALTH_APP.STAGING."Step_ExtSDM"                  (event_timestamp TIMESTAMP_NTZ, log_trigger STRING, process_id NUMBER, message STRING);
CREATE TABLE IF NOT EXISTS HEALTH_APP.STAGING."Step_FlushableStepDataCache"  (event_timestamp TIMESTAMP_NTZ, log_trigger STRING, process_id NUMBER, message STRING);
CREATE TABLE IF NOT EXISTS HEALTH_APP.STAGING."Step_HGNH"                    (event_timestamp TIMESTAMP_NTZ, log_trigger STRING, process_id NUMBER, message STRING);
CREATE TABLE IF NOT EXISTS HEALTH_APP.STAGING."Step_LSC"                     (event_timestamp TIMESTAMP_NTZ, log_trigger STRING, process_id NUMBER, message STRING);
CREATE TABLE IF NOT EXISTS HEALTH_APP.STAGING."Step_NotificationUtil"        (event_timestamp TIMESTAMP_NTZ, log_trigger STRING, process_id NUMBER, message STRING);
CREATE TABLE IF NOT EXISTS HEALTH_APP.STAGING."Step_SPUtils"                 (event_timestamp TIMESTAMP_NTZ, log_trigger STRING, process_id NUMBER, message STRING);
CREATE TABLE IF NOT EXISTS HEALTH_APP.STAGING."Step_ScreenUtil"              (event_timestamp TIMESTAMP_NTZ, log_trigger STRING, process_id NUMBER, message STRING);
CREATE TABLE IF NOT EXISTS HEALTH_APP.STAGING."Step_StandReportReceiver"     (event_timestamp TIMESTAMP_NTZ, log_trigger STRING, process_id NUMBER, message STRING);
CREATE TABLE IF NOT EXISTS HEALTH_APP.STAGING."Step_StandStepCounter"        (event_timestamp TIMESTAMP_NTZ, log_trigger STRING, process_id NUMBER, message STRING);
CREATE TABLE IF NOT EXISTS HEALTH_APP.STAGING."Step_StandStepDataManager"    (event_timestamp TIMESTAMP_NTZ, log_trigger STRING, process_id NUMBER, message STRING);

-- CONTROL.DATA_ANOMALIES — schéma non-métier dédié
CREATE TABLE IF NOT EXISTS HEALTH_APP.CONTROL.DATA_ANOMALIES (
    anomaly_id              NUMBER AUTOINCREMENT PRIMARY KEY,
    event_id                NUMBER,
    event_timestamp         TIMESTAMP_NTZ,
    process_name            STRING,
    process_id              NUMBER,
    message                 STRING,
    is_correct_timestamp    BOOLEAN,
    is_correct_process_name BOOLEAN,
    anomaly_type            VARCHAR(10),
    reason                  VARCHAR(500),
    detected_at             TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- CONTROL.TASK_LOGS — journal d'exécution
CREATE TABLE IF NOT EXISTS HEALTH_APP.CONTROL.TASK_LOGS (
    log_id          NUMBER AUTOINCREMENT PRIMARY KEY,
    task_name       VARCHAR(200),
    execution_time  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    rows_processed  NUMBER,
    status          VARCHAR(50),
    message         VARCHAR(1000)
);
```

---

### 📄 scripts/04_functions_procedures.sql

```sql
-- =============================================================
-- 04_functions_procedures.sql
-- UDFs + Procédures
-- Rôle requis : APP_ROLE
-- =============================================================

USE ROLE APP_ROLE;
USE DATABASE HEALTH_APP;
USE SCHEMA RAW;
USE WAREHOUSE PIPELINE_WH;

-- UDF : validation du timestamp
CREATE OR REPLACE FUNCTION HEALTH_APP.RAW.CHECK_CORRECT_TIMESTAMP(event_timestamp TIMESTAMP_NTZ)
RETURNS BOOLEAN LANGUAGE SQL AS
$$
    CASE
        WHEN event_timestamp IS NULL THEN FALSE
        WHEN event_timestamp > '2016-01-01 00:00:00'::TIMESTAMP_NTZ
         AND event_timestamp <= CURRENT_TIMESTAMP() THEN TRUE
        ELSE FALSE
    END
$$;

-- UDF : validation du process_name (whitelist)
CREATE OR REPLACE FUNCTION HEALTH_APP.RAW.CHECK_CORRECT_PROCESS_NAME(process_name STRING)
RETURNS BOOLEAN LANGUAGE SQL AS
$$
    CASE
        WHEN process_name IS NULL THEN FALSE
        WHEN process_name IN (
            'HiH_','HiH_ListenerManager','HiH_HiBroadcastUtil','HiH_DataStatManager',
            'HiH_HiAppUtil','HiH_HiHealthBinder','HiH_HiHealthDataInsertStore',
            'HiH_HiSyncControl','HiH_HiSyncUtil','Step_StandStepCounter','Step_SPUtils',
            'Step_NotificationUtil','Step_StandReportReceiver','Step_ScreenUtil',
            'Step_StandStepDataManager','Step_ExtSDM','Step_DataCache','Step_HGNH',
            'Step_FlushableStepDataCache','Step_LSC'
        ) THEN TRUE
        ELSE FALSE
    END
$$;

-- UDF Python : extraction du log_trigger
CREATE OR REPLACE FUNCTION HEALTH_APP.RAW.EXTRACT_LOG_MESSAGE(message STRING)
RETURNS STRING LANGUAGE PYTHON RUNTIME_VERSION = '3.12' HANDLER = 'extract_log_trigger'
AS $$
def extract_log_trigger(message: str):
    if message is None:
        return None
    return message.strip().split(" ")[0].split(":")[0].split("=")[0].strip()
$$;

-- UDF Python : extraction du reste du message
CREATE OR REPLACE FUNCTION HEALTH_APP.RAW.EXTRACT_LOG_REST(message STRING)
RETURNS STRING LANGUAGE PYTHON RUNTIME_VERSION = '3.12' HANDLER = 'extract_rest'
AS $$
def extract_rest(message: str):
    if message is None:
        return None
    msg = message.strip()
    if msg == "":
        return None
    trigger = msg.split(" ")[0].split(":")[0].split("=")[0].strip()
    rest = msg[len(trigger):] if msg.startswith(trigger) else " ".join(msg.split()[1:])
    rest = rest.lstrip(" :=").strip()
    return rest if rest != "" else None
$$;

-- PROCÉDURE : data_quality()
-- Lit le stream, envoie invalides + tardives → CONTROL.DATA_ANOMALIES
CREATE OR REPLACE PROCEDURE HEALTH_APP.RAW.DATA_QUALITY()
RETURNS STRING LANGUAGE SQL EXECUTE AS CALLER
AS
$$
DECLARE
    n_invalid NUMBER := 0;
    n_late    NUMBER := 0;
BEGIN
    -- Données INVALIDES (pas tardives)
    INSERT INTO HEALTH_APP.CONTROL.DATA_ANOMALIES (
        event_id, event_timestamp, process_name, process_id, message,
        is_correct_timestamp, is_correct_process_name, anomaly_type, reason
    )
    SELECT
        event_id, event_timestamp, process_name, process_id, message,
        HEALTH_APP.RAW.CHECK_CORRECT_TIMESTAMP(event_timestamp),
        HEALTH_APP.RAW.CHECK_CORRECT_PROCESS_NAME(process_name),
        'INVALID',
        CASE
            WHEN event_timestamp IS NULL
                THEN 'event_timestamp NULL'
            WHEN NOT HEALTH_APP.RAW.CHECK_CORRECT_TIMESTAMP(event_timestamp)
                THEN 'timestamp hors plage (avant 2016 ou futur)'
            WHEN process_name IS NULL
                THEN 'process_name NULL'
            WHEN NOT HEALTH_APP.RAW.CHECK_CORRECT_PROCESS_NAME(process_name)
                THEN 'process_name inconnu : ' || process_name
            ELSE 'donnée invalide'
        END
    FROM HEALTH_APP.RAW.RAW_EVENTS_STREAM
    WHERE DATEDIFF('day', event_timestamp, CURRENT_TIMESTAMP()) <= 5
      AND (
            NOT HEALTH_APP.RAW.CHECK_CORRECT_TIMESTAMP(event_timestamp)
         OR NOT HEALTH_APP.RAW.CHECK_CORRECT_PROCESS_NAME(process_name)
      );
    n_invalid := SQLROWCOUNT;

    -- Données TARDIVES (> 5 jours)
    INSERT INTO HEALTH_APP.CONTROL.DATA_ANOMALIES (
        event_id, event_timestamp, process_name, process_id, message,
        is_correct_timestamp, is_correct_process_name, anomaly_type, reason
    )
    SELECT
        event_id, event_timestamp, process_name, process_id, message,
        HEALTH_APP.RAW.CHECK_CORRECT_TIMESTAMP(event_timestamp),
        HEALTH_APP.RAW.CHECK_CORRECT_PROCESS_NAME(process_name),
        'LATE',
        'Donnée tardive : '
            || DATEDIFF('day', event_timestamp, CURRENT_TIMESTAMP())::STRING
            || ' jours de retard'
    FROM HEALTH_APP.RAW.RAW_EVENTS_STREAM
    WHERE event_timestamp IS NOT NULL
      AND DATEDIFF('day', event_timestamp, CURRENT_TIMESTAMP()) > 5;
    n_late := SQLROWCOUNT;

    INSERT INTO HEALTH_APP.CONTROL.TASK_LOGS (task_name, rows_processed, status, message)
    VALUES ('DATA_QUALITY', n_invalid + n_late, 'SUCCESS',
            'invalides=' || n_invalid || ' tardives=' || n_late);

    RETURN 'OK invalides=' || n_invalid || ' tardives=' || n_late;
END;
$$;

-- PROCÉDURE : enrich_data(task_key, target_process_name)
-- Charge uniquement les lignes VALIDES + NON TARDIVES dans STAGING
CREATE OR REPLACE PROCEDURE HEALTH_APP.RAW.ENRICH_DATA(
    task_key            VARCHAR,
    target_process_name VARCHAR
)
RETURNS STRING LANGUAGE SQL EXECUTE AS CALLER
AS
$$
DECLARE
    v_rows NUMBER;
    v_sql  STRING;
BEGIN
    v_sql := '
        INSERT INTO HEALTH_APP.STAGING."' || target_process_name || '"
            (event_timestamp, log_trigger, process_id, message)
        WITH base AS (
            SELECT event_timestamp, process_id, message, SPLIT(message, '' '') AS message_array
            FROM HEALTH_APP.RAW.RAW_EVENTS_STREAM
            WHERE process_name = ''' || target_process_name || '''
              AND HEALTH_APP.RAW.CHECK_CORRECT_TIMESTAMP(event_timestamp)    = TRUE
              AND HEALTH_APP.RAW.CHECK_CORRECT_PROCESS_NAME(process_name)    = TRUE
              AND DATEDIFF(''day'', event_timestamp, CURRENT_TIMESTAMP()) <= 5
        ),
        parsed AS (
            SELECT
                event_timestamp,
                process_id,
                HEALTH_APP.RAW.EXTRACT_LOG_MESSAGE(message) AS log_trigger,
                HEALTH_APP.RAW.EXTRACT_LOG_REST(message)    AS message
            FROM base
        )
        SELECT event_timestamp, log_trigger, process_id, message FROM parsed
    ';
    EXECUTE IMMEDIATE v_sql;
    v_rows := SQLROWCOUNT;

    INSERT INTO HEALTH_APP.CONTROL.TASK_LOGS (task_name, rows_processed, status, message)
    VALUES ('ENRICH_DATA__' || task_key, v_rows, 'SUCCESS',
            'process=' || target_process_name || ' rows=' || v_rows);

    RETURN 'OK ' || task_key || ' (' || target_process_name || ') inserted=' || v_rows;
END;
$$;
```

---

### 📄 scripts/05_tasks.sql

```sql
-- =============================================================
-- 05_tasks.sql
-- 21 TASKs créées avec APP_ROLE
-- =============================================================

USE ROLE APP_ROLE;
USE DATABASE HEALTH_APP;
USE SCHEMA RAW;
USE WAREHOUSE PIPELINE_WH;

-- TASK RACINE
CREATE OR REPLACE TASK HEALTH_APP.RAW.DATA_QUALITY_TASK
    WAREHOUSE = PIPELINE_WH
    SCHEDULE  = '60 MINUTE'
AS CALL HEALTH_APP.RAW.DATA_QUALITY();

-- TASKS ENFANTS (AFTER data_quality_task)
CREATE OR REPLACE TASK HEALTH_APP.RAW.ENRICH_DATA_HIH_
    WAREHOUSE = PIPELINE_WH AFTER HEALTH_APP.RAW.DATA_QUALITY_TASK
AS CALL HEALTH_APP.RAW.ENRICH_DATA('hih_', 'HiH_');

CREATE OR REPLACE TASK HEALTH_APP.RAW.ENRICH_DATA_HIH_LISTENER_MANAGER
    WAREHOUSE = PIPELINE_WH AFTER HEALTH_APP.RAW.DATA_QUALITY_TASK
AS CALL HEALTH_APP.RAW.ENRICH_DATA('hih_listener_manager', 'HiH_ListenerManager');

CREATE OR REPLACE TASK HEALTH_APP.RAW.ENRICH_DATA_HIH_HIBROADCASTUTIL
    WAREHOUSE = PIPELINE_WH AFTER HEALTH_APP.RAW.DATA_QUALITY_TASK
AS CALL HEALTH_APP.RAW.ENRICH_DATA('hih_hi_broadcast_util', 'HiH_HiBroadcastUtil');

CREATE OR REPLACE TASK HEALTH_APP.RAW.ENRICH_DATA_HIH_DATASTATMANAGER
    WAREHOUSE = PIPELINE_WH AFTER HEALTH_APP.RAW.DATA_QUALITY_TASK
AS CALL HEALTH_APP.RAW.ENRICH_DATA('hih_data_stat_manager', 'HiH_DataStatManager');

CREATE OR REPLACE TASK HEALTH_APP.RAW.ENRICH_DATA_HIH_HIAPPUTIL
    WAREHOUSE = PIPELINE_WH AFTER HEALTH_APP.RAW.DATA_QUALITY_TASK
AS CALL HEALTH_APP.RAW.ENRICH_DATA('hih_hi_app_util', 'HiH_HiAppUtil');

CREATE OR REPLACE TASK HEALTH_APP.RAW.ENRICH_DATA_HIH_HIHEALTHBINDER
    WAREHOUSE = PIPELINE_WH AFTER HEALTH_APP.RAW.DATA_QUALITY_TASK
AS CALL HEALTH_APP.RAW.ENRICH_DATA('hih_hi_health_binder', 'HiH_HiHealthBinder');

CREATE OR REPLACE TASK HEALTH_APP.RAW.ENRICH_DATA_HIH_HIHEALTHDATAINSERTSTORE
    WAREHOUSE = PIPELINE_WH AFTER HEALTH_APP.RAW.DATA_QUALITY_TASK
AS CALL HEALTH_APP.RAW.ENRICH_DATA('hih_hi_health_data_insert_store', 'HiH_HiHealthDataInsertStore');

CREATE OR REPLACE TASK HEALTH_APP.RAW.ENRICH_DATA_HIH_HISYNCCONTROL
    WAREHOUSE = PIPELINE_WH AFTER HEALTH_APP.RAW.DATA_QUALITY_TASK
AS CALL HEALTH_APP.RAW.ENRICH_DATA('hih_hi_sync_control', 'HiH_HiSyncControl');

CREATE OR REPLACE TASK HEALTH_APP.RAW.ENRICH_DATA_HIH_HISYNCUTIL
    WAREHOUSE = PIPELINE_WH AFTER HEALTH_APP.RAW.DATA_QUALITY_TASK
AS CALL HEALTH_APP.RAW.ENRICH_DATA('hih_hi_sync_util', 'HiH_HiSyncUtil');

CREATE OR REPLACE TASK HEALTH_APP.RAW.ENRICH_DATA_STEP_DATACACHE
    WAREHOUSE = PIPELINE_WH AFTER HEALTH_APP.RAW.DATA_QUALITY_TASK
AS CALL HEALTH_APP.RAW.ENRICH_DATA('step_data_cache', 'Step_DataCache');

CREATE OR REPLACE TASK HEALTH_APP.RAW.ENRICH_DATA_STEP_EXTSDM
    WAREHOUSE = PIPELINE_WH AFTER HEALTH_APP.RAW.DATA_QUALITY_TASK
AS CALL HEALTH_APP.RAW.ENRICH_DATA('step_ext_sdm', 'Step_ExtSDM');

CREATE OR REPLACE TASK HEALTH_APP.RAW.ENRICH_DATA_STEP_FLUSHABLESTEPDATACACHE
    WAREHOUSE = PIPELINE_WH AFTER HEALTH_APP.RAW.DATA_QUALITY_TASK
AS CALL HEALTH_APP.RAW.ENRICH_DATA('step_flushable_step_data_cache', 'Step_FlushableStepDataCache');

CREATE OR REPLACE TASK HEALTH_APP.RAW.ENRICH_DATA_STEP_HGNH
    WAREHOUSE = PIPELINE_WH AFTER HEALTH_APP.RAW.DATA_QUALITY_TASK
AS CALL HEALTH_APP.RAW.ENRICH_DATA('step_hgnh', 'Step_HGNH');

CREATE OR REPLACE TASK HEALTH_APP.RAW.ENRICH_DATA_STEP_LSC
    WAREHOUSE = PIPELINE_WH AFTER HEALTH_APP.RAW.DATA_QUALITY_TASK
AS CALL HEALTH_APP.RAW.ENRICH_DATA('step_lsc', 'Step_LSC');

CREATE OR REPLACE TASK HEALTH_APP.RAW.ENRICH_DATA_STEP_NOTIFICATIONUTIL
    WAREHOUSE = PIPELINE_WH AFTER HEALTH_APP.RAW.DATA_QUALITY_TASK
AS CALL HEALTH_APP.RAW.ENRICH_DATA('step_notification_util', 'Step_NotificationUtil');

CREATE OR REPLACE TASK HEALTH_APP.RAW.ENRICH_DATA_STEP_SPUTILS
    WAREHOUSE = PIPELINE_WH AFTER HEALTH_APP.RAW.DATA_QUALITY_TASK
AS CALL HEALTH_APP.RAW.ENRICH_DATA('step_sp_utils', 'Step_SPUtils');

CREATE OR REPLACE TASK HEALTH_APP.RAW.ENRICH_DATA_STEP_SCREENUTIL
    WAREHOUSE = PIPELINE_WH AFTER HEALTH_APP.RAW.DATA_QUALITY_TASK
AS CALL HEALTH_APP.RAW.ENRICH_DATA('step_screen_util', 'Step_ScreenUtil');

CREATE OR REPLACE TASK HEALTH_APP.RAW.ENRICH_DATA_STEP_STANDREPORTRECEIVER
    WAREHOUSE = PIPELINE_WH AFTER HEALTH_APP.RAW.DATA_QUALITY_TASK
AS CALL HEALTH_APP.RAW.ENRICH_DATA('step_stand_report_receiver', 'Step_StandReportReceiver');

CREATE OR REPLACE TASK HEALTH_APP.RAW.ENRICH_DATA_STEP_STANDSTEPCOUNTER
    WAREHOUSE = PIPELINE_WH AFTER HEALTH_APP.RAW.DATA_QUALITY_TASK
AS CALL HEALTH_APP.RAW.ENRICH_DATA('step_stand_step_counter', 'Step_StandStepCounter');

CREATE OR REPLACE TASK HEALTH_APP.RAW.ENRICH_DATA_STEP_STANDSTEPDATAMANAGER
    WAREHOUSE = PIPELINE_WH AFTER HEALTH_APP.RAW.DATA_QUALITY_TASK
AS CALL HEALTH_APP.RAW.ENRICH_DATA('step_stand_step_data_manager', 'Step_StandStepDataManager');

-- ACTIVATION — enfants d'abord, racine en dernier
ALTER TASK HEALTH_APP.RAW.ENRICH_DATA_HIH_                          RESUME;
ALTER TASK HEALTH_APP.RAW.ENRICH_DATA_HIH_LISTENER_MANAGER          RESUME;
ALTER TASK HEALTH_APP.RAW.ENRICH_DATA_HIH_HIBROADCASTUTIL           RESUME;
ALTER TASK HEALTH_APP.RAW.ENRICH_DATA_HIH_DATASTATMANAGER           RESUME;
ALTER TASK HEALTH_APP.RAW.ENRICH_DATA_HIH_HIAPPUTIL                 RESUME;
ALTER TASK HEALTH_APP.RAW.ENRICH_DATA_HIH_HIHEALTHBINDER            RESUME;
ALTER TASK HEALTH_APP.RAW.ENRICH_DATA_HIH_HIHEALTHDATAINSERTSTORE   RESUME;
ALTER TASK HEALTH_APP.RAW.ENRICH_DATA_HIH_HISYNCCONTROL             RESUME;
ALTER TASK HEALTH_APP.RAW.ENRICH_DATA_HIH_HISYNCUTIL                RESUME;
ALTER TASK HEALTH_APP.RAW.ENRICH_DATA_STEP_DATACACHE                RESUME;
ALTER TASK HEALTH_APP.RAW.ENRICH_DATA_STEP_EXTSDM                   RESUME;
ALTER TASK HEALTH_APP.RAW.ENRICH_DATA_STEP_FLUSHABLESTEPDATACACHE   RESUME;
ALTER TASK HEALTH_APP.RAW.ENRICH_DATA_STEP_HGNH                     RESUME;
ALTER TASK HEALTH_APP.RAW.ENRICH_DATA_STEP_LSC                      RESUME;
ALTER TASK HEALTH_APP.RAW.ENRICH_DATA_STEP_NOTIFICATIONUTIL         RESUME;
ALTER TASK HEALTH_APP.RAW.ENRICH_DATA_STEP_SPUTILS                  RESUME;
ALTER TASK HEALTH_APP.RAW.ENRICH_DATA_STEP_SCREENUTIL               RESUME;
ALTER TASK HEALTH_APP.RAW.ENRICH_DATA_STEP_STANDREPORTRECEIVER      RESUME;
ALTER TASK HEALTH_APP.RAW.ENRICH_DATA_STEP_STANDSTEPCOUNTER         RESUME;
ALTER TASK HEALTH_APP.RAW.ENRICH_DATA_STEP_STANDSTEPDATAMANAGER     RESUME;
ALTER TASK HEALTH_APP.RAW.DATA_QUALITY_TASK                         RESUME;
```

---

### 📄 scripts/06_test_data.sql

```sql
-- =============================================================
-- 06_test_data.sql
-- Données de test (3 cas)
-- Rôle requis : APP_ROLE
-- =============================================================

USE ROLE APP_ROLE;
USE DATABASE HEALTH_APP;
USE WAREHOUSE PIPELINE_WH;

-- CAS 1 : VALIDES
INSERT INTO HEALTH_APP.RAW.RAW_EVENTS (event_timestamp, process_name, process_id, message) VALUES
    (DATEADD('hour',  -1, CURRENT_TIMESTAMP()), 'HiH_ListenerManager',   1001, 'onReceive data=12345'),
    (DATEADD('hour', -12, CURRENT_TIMESTAMP()), 'Step_LSC',              1002, 'loadData count=42'),
    (DATEADD('day',   -2, CURRENT_TIMESTAMP()), 'Step_StandStepCounter', 1003, 'stepCount=8500'),
    (DATEADD('day',   -4, CURRENT_TIMESTAMP()), 'HiH_HiSyncUtil',        1004, 'syncDone status=OK');

-- CAS 2 : INVALIDES
INSERT INTO HEALTH_APP.RAW.RAW_EVENTS (event_timestamp, process_name, process_id, message) VALUES
    (NULL,                                                    'HiH_ListenerManager', 1010, 'timestamp null'),
    ('2010-06-15 10:00:00'::TIMESTAMP_NTZ,                   'Step_LSC',            1011, 'timestamp avant 2016'),
    (DATEADD('hour', -2, CURRENT_TIMESTAMP()),                'UnknownProcess_XYZ',  1012, 'process inconnu'),
    (DATEADD('hour', -3, CURRENT_TIMESTAMP()),                NULL,                  1013, 'process null');

-- CAS 3 : TARDIFS (> 5 jours)
INSERT INTO HEALTH_APP.RAW.RAW_EVENTS (event_timestamp, process_name, process_id, message) VALUES
    (DATEADD('day',  -6, CURRENT_TIMESTAMP()), 'HiH_DataStatManager', 1020, 'statReport late 6j'),
    (DATEADD('day', -10, CURRENT_TIMESTAMP()), 'Step_HGNH',           1021, 'heartbeat late 10j'),
    (DATEADD('day', -30, CURRENT_TIMESTAMP()), 'Step_ScreenUtil',     1022, 'screen event late 30j');

-- Exécution manuelle
EXECUTE TASK HEALTH_APP.RAW.DATA_QUALITY_TASK;
```

---

### 📄 deploy.sql

```sql
-- =============================================================
-- deploy.sql — Script principal
-- Usage : snowsql -a <account> -u <user> -f deploy.sql
-- =============================================================

USE ROLE ACCOUNTADMIN;

!source scripts/01_setup.sql
!source scripts/02_rbac.sql
!source scripts/03_tables.sql
!source scripts/04_functions_procedures.sql
!source scripts/05_tasks.sql

-- Tests (optionnel) :
-- !source scripts/06_test_data.sql
```

---

*Projet réalisé dans le cadre du cours Snowflake – Pipeline ETL avancé.*

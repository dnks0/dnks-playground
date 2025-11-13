-- Databricks notebook source
-- Create a schema
CREATE CATALOG IF NOT EXISTS dnks;
CREATE SCHEMA IF NOT EXISTS dnks.abac_rls_at_scale;

-- COMMAND ----------

-- Create the table with specified schema
CREATE TABLE IF NOT EXISTS dnks.abac_rls_at_scale.data (
    country STRING,
    plant STRING,
    dealer STRING,
    vehicle_model STRING,
    data STRING
);

-- COMMAND ----------

-- Insert 10 records with random data
INSERT INTO dnks.abac_rls_at_scale.data VALUES
    ('DE', 'plant_1', 'dealer_1', 'model_1', 'abc123def456ghi789jkl012mno345p'),
    ('FR', 'plant_2', 'dealer_2', 'model_2', 'xyz789uvw456rst123opq890lmn567k'),
    ('US', 'plant_3', 'dealer_3', 'model_1', 'qwe456rty123uio789asd012fgh345j'),
    ('DE', 'plant_2', 'dealer_4', 'model_2', 'zxc987vbn654poi321mnb098lkj876h'),
    ('FR', 'plant_3', 'dealer_5', 'model_1', 'ghi321jkl654mno987pqr210stu543v'),
    ('US', 'plant_1', 'dealer_1', 'model_2', 'wer765tyu098iop432sdf876ghj109k'),
    ('DE', 'plant_3', 'dealer_2', 'model_1', 'bvc543nfg876hjk210qaz987wsx654e'),
    ('FR', 'plant_1', 'dealer_3', 'model_2', 'edc432rfv765tgb098yhn543mju876i'),
    ('US', 'plant_2', 'dealer_4', 'model_1', 'cde789fgh123jkl456mno789pqr012s'),
    ('DE', 'plant_3', 'dealer_5', 'model_2', 'vfr654tgb987yhn321ujm098iko765p');


-- COMMAND ----------

-- Verify the data was inserted correctly
SELECT * FROM dnks.abac_rls_at_scale.data;

-- COMMAND ----------

-- Create the filters table - defines what each filter allows
CREATE TABLE IF NOT EXISTS dnks.abac_rls_at_scale.filters (
    filter_id STRING,
    filter_name STRING,
    filter_type STRING,
    filter_value STRING
);

-- COMMAND ----------

-- Insert filter definitions by type
INSERT INTO dnks.abac_rls_at_scale.filters VALUES
    (001, 'admin', 'global', '*'),
    (002, 'country-germany', 'country', 'DE'),
    (003, 'country-all', 'country', '*'),
    (004, 'plant-plant1', 'plant', 'plant_1');

-- COMMAND ----------

SELECT * FROM dnks.abac_rls_at_scale.filters

-- COMMAND ----------

-- Create roles table - defines reusable roles
CREATE TABLE IF NOT EXISTS dnks.abac_rls_at_scale.roles (
    role_id STRING,
    role_name STRING,
    description STRING
);

-- COMMAND ----------

-- Insert role definitions
INSERT INTO dnks.abac_rls_at_scale.roles VALUES
    (001, 'Role A', 'Full access to all data'),
    (002, 'Role B', 'Access to specific data'),
    (003, 'Role C', 'Access to specific data'),
    (004, 'Role D', 'Access to specific data');


-- COMMAND ----------

SELECT * FROM dnks.abac_rls_at_scale.roles;

-- COMMAND ----------

-- Create role to filter mapping - defines what filters each role includes
CREATE TABLE IF NOT EXISTS dnks.abac_rls_at_scale.role_filter_mapping (
    role_id STRING,
    filter_id STRING
);

-- COMMAND ----------

INSERT INTO dnks.abac_rls_at_scale.role_filter_mapping VALUES
    (001, 001),
    (002, 002),
    (002, 004),
    (003, 003),
    (004, 004);

-- COMMAND ----------

SELECT * FROM dnks.abac_rls_at_scale.role_filter_mapping;

-- COMMAND ----------

-- Create principal to role mapping - assigns roles to principals
CREATE TABLE IF NOT EXISTS dnks.abac_rls_at_scale.principal_role_mapping (
    principal STRING,
    role_id STRING
);

-- COMMAND ----------

INSERT INTO dnks.abac_rls_at_scale.principal_role_mapping VALUES
    ('dominik.schuessele@databricks.com', 002);

-- COMMAND ----------

SELECT * FROM dnks.abac_rls_at_scale.principal_role_mapping;

-- COMMAND ----------

SELECT filter_type, COUNT(*) as count FROM dnks.abac_rls_at_scale.filters GROUP BY filter_type ORDER BY filter_type;

-- COMMAND ----------

SELECT principal, STRING_AGG(role_id, ', ') as roles
FROM dnks.abac_rls_at_scale.principal_role_mapping
GROUP BY principal 
ORDER BY principal;

-- COMMAND ----------

SELECT
  prm.principal,
  prm.role_id,
  rfm.filter_id,
  f.filter_name,
  f.filter_type,
  f.filter_value
FROM dnks.abac_rls_at_scale.principal_role_mapping prm
INNER JOIN dnks.abac_rls_at_scale.role_filter_mapping rfm ON prm.role_id = rfm.role_id
INNER JOIN dnks.abac_rls_at_scale.filters f ON rfm.filter_id = f.filter_id
WHERE prm.principal = current_user()

-- COMMAND ----------

-- Unity Catalog Function for ABAC Row Filter
-- This function checks if the current user has access to a specific row
CREATE OR REPLACE FUNCTION dnks.abac_rls_at_scale.country_access(
    rcountry STRING
)
RETURNS BOOLEAN
LANGUAGE SQL
DETERMINISTIC
READS SQL DATA
COMMENT 'Unity Catalog ABAC function to check data access based on privileges'
RETURN (
  SELECT EXISTS (
    SELECT 1 
    FROM (
      SELECT
        prm.principal,
        prm.role_id,
        rfm.filter_id,
        f.filter_name,
        f.filter_type,
        f.filter_value
      FROM dnks.abac_rls_at_scale.principal_role_mapping prm
      INNER JOIN dnks.abac_rls_at_scale.role_filter_mapping rfm ON prm.role_id = rfm.role_id
      INNER JOIN dnks.abac_rls_at_scale.filters f ON rfm.filter_id = f.filter_id
      WHERE prm.principal = current_user()
      AND f.filter_type = 'country'
    ) pf
    WHERE (pf.filter_value = '*' OR pf.filter_value = rcountry)
  )
);

-- COMMAND ----------

-- Unity Catalog Function for ABAC Row Filter
-- This function checks if the current user has access to a specific row
CREATE OR REPLACE FUNCTION dnks.abac_rls_at_scale.plant_access(
    rplant STRING
)
RETURNS BOOLEAN
LANGUAGE SQL
DETERMINISTIC
READS SQL DATA
COMMENT 'Unity Catalog ABAC function to check data access based on privileges'
RETURN (
  SELECT EXISTS (
    SELECT 1 
    FROM (
      SELECT
        prm.principal,
        prm.role_id,
        rfm.filter_id,
        f.filter_name,
        f.filter_type,
        f.filter_value
      FROM dnks.abac_rls_at_scale.principal_role_mapping prm
      INNER JOIN dnks.abac_rls_at_scale.role_filter_mapping rfm ON prm.role_id = rfm.role_id
      INNER JOIN dnks.abac_rls_at_scale.filters f ON rfm.filter_id = f.filter_id
      WHERE prm.principal = current_user()
      AND f.filter_type = 'plant'
    ) pf
    WHERE (pf.filter_value = '*' OR pf.filter_value = rplant)
  )
);

-- COMMAND ----------

SELECT dnks.abac_rls_at_scale.country_access('DE');

-- COMMAND ----------

SELECT dnks.abac_rls_at_scale.plant_access('plant_1');

-- COMMAND ----------

CREATE OR REPLACE POLICY `country-policy`
ON CATALOG bu_1
COMMENT 'Hides rows based on allowed access to country information'
ROW FILTER dnks.abac_rls_at_scale.country_access
TO `account users`
FOR TABLES
MATCH COLUMNS hasTagValue('filter', 'country') AS rcountry
USING COLUMNS (rcountry);

-- COMMAND ----------

CREATE OR REPLACE POLICY `plant-policy`
ON CATALOG bu_1
COMMENT 'Hides rows based on allowed access to plant information'
ROW FILTER dnks.abac_rls_at_scale.plant_access
TO `account users`
FOR TABLES
MATCH COLUMNS hasTagValue('filter', 'plant') AS rplant
USING COLUMNS (rplant);

-- COMMAND ----------

-- optional
-- manually create a tag policy allowing only `filter_type` values for tag `filter`

-- COMMAND ----------

ALTER TABLE dnks.abac_rls_at_scale.`table`
ALTER COLUMN country
SET TAGS ('filter' = 'country');

-- COMMAND ----------

ALTER TABLE dnks.abac_rls_at_scale.`table`
ALTER COLUMN plant
SET TAGS ('filter' = 'plant');

-- COMMAND ----------

SELECT * FROM dnks.abac_rls_at_scale.table;

-- COMMAND ----------

INSERT INTO dnks.abac_rls_at_scale.filters VALUES
    (005, 'country-fr', 'country', 'FR');

INSERT INTO dnks.abac_rls_at_scale.role_filter_mapping VALUES
    (002, 005);

-- COMMAND ----------

INSERT INTO dnks.abac_rls_at_scale.filters VALUES
    (006, 'plant-plant2', 'plant', 'plant_2');

INSERT INTO dnks.abac_rls_at_scale.role_filter_mapping VALUES
    (002, 006);

-- COMMAND ----------

SELECT * FROM dnks.abac_rls_at_scale.table;

-- COMMAND ----------

SELECT * fROM dnks.abac_rls_at_scale.filters;

-- Development-only credentials; production roles/passwords are provisioned separately.
CREATE ROLE lake_catalog LOGIN PASSWORD 'catalog_local';
CREATE ROLE lake_audit LOGIN PASSWORD 'audit_local';
CREATE ROLE airflow LOGIN PASSWORD 'airflow_local';
CREATE DATABASE lakebridge;
CREATE DATABASE airflow OWNER airflow;
\connect lakebridge
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
CREATE SCHEMA catalog AUTHORIZATION lake_catalog;
CREATE SCHEMA audit AUTHORIZATION lake_audit;
ALTER ROLE lake_catalog IN DATABASE lakebridge SET search_path = catalog;
ALTER ROLE lake_audit IN DATABASE lakebridge SET search_path = audit;
GRANT CONNECT ON DATABASE lakebridge TO lake_catalog, lake_audit;


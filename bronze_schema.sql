CREATE WAREHOUSE my_warehouse
WITH WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE;

CREATE DATABASE data_project;

CREATE SCHEMA data_project.bronze;
CREATE SCHEMA data_project.silver;
CREATE SCHEMA data_project.gold;
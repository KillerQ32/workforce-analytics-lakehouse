CREATE CATALOG IF NOT EXISTS workforce_analytics;

CREATE SCHEMA IF NOT EXISTS workforce_analytics.bronze;
CREATE SCHEMA IF NOT EXISTS workforce_analytics.silver;
CREATE SCHEMA IF NOT EXISTS workforce_analytics.gold;

CREATE VOLUME IF NOT EXISTS workforce_analytics.bronze.raw_files;
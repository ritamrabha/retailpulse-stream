#!/bin/bash
# Runs once, on the first start of an empty MySQL volume (after 01_schema.sql and 02_views.sql).
# Creates the read-only user "tableau_ro" that can read ONLY the fast dash_* views.
set -e
mysql -uroot -p"$MYSQL_ROOT_PASSWORD" <<SQL
CREATE USER IF NOT EXISTS 'tableau_ro'@'%' IDENTIFIED BY '${TABLEAU_RO_PASSWORD}';
GRANT SELECT ON ecommerce_bdv.dash_kpi_overall    TO 'tableau_ro'@'%';
GRANT SELECT ON ecommerce_bdv.dash_funnel         TO 'tableau_ro'@'%';
GRANT SELECT ON ecommerce_bdv.dash_daily          TO 'tableau_ro'@'%';
GRANT SELECT ON ecommerce_bdv.dash_hourly         TO 'tableau_ro'@'%';
GRANT SELECT ON ecommerce_bdv.dash_product        TO 'tableau_ro'@'%';
GRANT SELECT ON ecommerce_bdv.dash_category       TO 'tableau_ro'@'%';
GRANT SELECT ON ecommerce_bdv.dash_replay_monitor TO 'tableau_ro'@'%';
FLUSH PRIVILEGES;
SQL

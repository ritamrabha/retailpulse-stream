-- 03_tableau_readonly_user.sql
-- A read-only MySQL user for Tableau: it can read ONLY the fast dash_* views.
-- 1. COPY this file to  sql/03_tableau_readonly_user.local.sql  (git ignores *.local.sql files).
-- 2. In the COPY, replace CHANGE_ME with a password of your own (both CREATE USER lines).
-- 3. Run the copy:   mysql> source sql/03_tableau_readonly_user.local.sql
-- Never put a real password into this tracked file.

CREATE USER IF NOT EXISTS 'tableau_ro'@'localhost' IDENTIFIED BY 'CHANGE_ME';
CREATE USER IF NOT EXISTS 'tableau_ro'@'127.0.0.1' IDENTIFIED BY 'CHANGE_ME';

GRANT SELECT ON ecommerce_bdv.dash_kpi_overall   TO 'tableau_ro'@'localhost', 'tableau_ro'@'127.0.0.1';
GRANT SELECT ON ecommerce_bdv.dash_funnel        TO 'tableau_ro'@'localhost', 'tableau_ro'@'127.0.0.1';
GRANT SELECT ON ecommerce_bdv.dash_daily         TO 'tableau_ro'@'localhost', 'tableau_ro'@'127.0.0.1';
GRANT SELECT ON ecommerce_bdv.dash_hourly        TO 'tableau_ro'@'localhost', 'tableau_ro'@'127.0.0.1';
GRANT SELECT ON ecommerce_bdv.dash_product       TO 'tableau_ro'@'localhost', 'tableau_ro'@'127.0.0.1';
GRANT SELECT ON ecommerce_bdv.dash_category      TO 'tableau_ro'@'localhost', 'tableau_ro'@'127.0.0.1';
GRANT SELECT ON ecommerce_bdv.dash_replay_monitor TO 'tableau_ro'@'localhost', 'tableau_ro'@'127.0.0.1';
FLUSH PRIVILEGES;

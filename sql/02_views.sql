-- 02_views.sql
-- Creates the views. Run after 01_schema.sql.
--   dash_*  : fast views over the summary tables - Tableau reads these.
--   vw_*    : slow views that count the raw events - used only to validate the dash_* views.

USE ecommerce_bdv;

-- ===================== dash_* : fast, for Tableau =====================
CREATE OR REPLACE VIEW dash_kpi_overall AS
SELECT d.total_events, v.visitors, v.visitors_viewed, v.visitors_carted, v.visitors_bought,
       d.views, d.carts, d.purchases,
       (SELECT COUNT(DISTINCT transaction_id) FROM events WHERE event_type = 'transaction') AS transactions,
       ROUND(100 * d.carts / NULLIF(d.views, 0), 2)               AS view_to_cart_pct,
       ROUND(100 * d.purchases / NULLIF(d.carts, 0), 2)           AS cart_to_purchase_pct,
       ROUND(100 * v.visitors_bought / NULLIF(v.visitors, 0), 2)  AS visitors_who_bought_pct,
       (SELECT MAX(event_time) FROM events)                       AS data_up_to,
       (SELECT MAX(batch_id) FROM events)                         AS latest_batch
FROM (SELECT SUM(total_events) AS total_events, SUM(views) AS views, SUM(carts) AS carts,
             SUM(purchases) AS purchases FROM sum_daily) d
CROSS JOIN (SELECT COUNT(*) AS visitors, SUM(viewed) AS visitors_viewed, SUM(carted) AS visitors_carted,
                   SUM(bought) AS visitors_bought FROM sum_visitor) v;

CREATE OR REPLACE VIEW dash_funnel AS
SELECT 1 AS stage_order, '1 Viewed' AS stage, visitors_viewed AS visitors, views AS events FROM dash_kpi_overall
UNION ALL SELECT 2, '2 Added to cart', visitors_carted, carts FROM dash_kpi_overall
UNION ALL SELECT 3, '3 Purchased', visitors_bought, purchases FROM dash_kpi_overall;

CREATE OR REPLACE VIEW dash_daily AS
SELECT event_date, total_events, views, carts, purchases FROM sum_daily;

CREATE OR REPLACE VIEW dash_hourly AS
SELECT day_of_week, ELT(day_of_week + 1, 'Mon','Tue','Wed','Thu','Fri','Sat','Sun') AS day_name,
       event_hour, total_events, views, carts, purchases
FROM sum_hourly;

CREATE OR REPLACE VIEW dash_product AS
SELECT item_id, views, carts, purchases,
       ROUND(100 * carts / NULLIF(views, 0), 2) AS view_to_cart_pct
FROM sum_product;

CREATE OR REPLACE VIEW dash_category AS
SELECT s.category_id,
       IF(s.category_id = -1, 'No category', CAST(s.category_id AS CHAR)) AS category_label,
       c.level AS category_level, c.parent_id, c.root_category_id,
       IF(s.category_id = -1, 'No category',
          COALESCE(CAST(c.root_category_id AS CHAR), 'Hierarchy unknown')) AS top_level_label,
       s.total_events, s.views, s.carts, s.purchases
FROM sum_category s
LEFT JOIN categories c ON c.category_id = s.category_id;

CREATE OR REPLACE VIEW dash_replay_monitor AS
SELECT batch_id,
       IF(batch_id = 0, 'Baseline', CONCAT('Batch ', batch_id)) AS batch_label,
       week_start, batch_start, batch_end,
       events_planned, events_loaded, views_loaded, carts_loaded, purchases_loaded,
       loaded_at, summaries_done_at, graph_done_at,
       CASE WHEN batch_id = 0 THEN 'Baseline'
            WHEN graph_done_at IS NOT NULL THEN 'Complete'
            WHEN loaded_at IS NOT NULL THEN 'Partly applied'
            ELSE 'Waiting' END AS status
FROM replay_log;

-- ===================== vw_* : slow, for validation only =====================
CREATE OR REPLACE VIEW vw_kpi_overall AS
SELECT
  COUNT(*)                                                             AS total_events,
  COUNT(DISTINCT visitor_id)                                           AS visitors,
  COUNT(DISTINCT CASE WHEN event_type='view'        THEN visitor_id END) AS visitors_viewed,
  COUNT(DISTINCT CASE WHEN event_type='addtocart'   THEN visitor_id END) AS visitors_carted,
  COUNT(DISTINCT CASE WHEN event_type='transaction' THEN visitor_id END) AS visitors_bought,
  COUNT(CASE WHEN event_type='view'        THEN 1 END)                 AS views,
  COUNT(CASE WHEN event_type='addtocart'   THEN 1 END)                 AS carts,
  COUNT(CASE WHEN event_type='transaction' THEN 1 END)                 AS purchases,
  COUNT(DISTINCT transaction_id)                                       AS transactions,
  ROUND(100 * COUNT(CASE WHEN event_type='addtocart' THEN 1 END)
            / NULLIF(COUNT(CASE WHEN event_type='view' THEN 1 END), 0), 2)        AS view_to_cart_pct,
  ROUND(100 * COUNT(CASE WHEN event_type='transaction' THEN 1 END)
            / NULLIF(COUNT(CASE WHEN event_type='addtocart' THEN 1 END), 0), 2)   AS cart_to_purchase_pct,
  ROUND(100 * COUNT(DISTINCT CASE WHEN event_type='transaction' THEN visitor_id END)
            / NULLIF(COUNT(DISTINCT visitor_id), 0), 2)                           AS visitors_who_bought_pct,
  MAX(event_time)                                                      AS data_up_to,
  MAX(batch_id)                                                        AS latest_batch
FROM events;

CREATE OR REPLACE VIEW vw_funnel AS
WITH k AS (SELECT * FROM vw_kpi_overall)
SELECT 1 AS stage_order, '1 Viewed' AS stage, visitors_viewed AS visitors, views AS events FROM k
UNION ALL SELECT 2, '2 Added to cart', visitors_carted, carts FROM k
UNION ALL SELECT 3, '3 Purchased', visitors_bought, purchases FROM k;

CREATE OR REPLACE VIEW vw_daily_kpis AS
SELECT event_date,
       COUNT(*)                                             AS total_events,
       COUNT(DISTINCT visitor_id)                           AS active_visitors,
       COUNT(CASE WHEN event_type='view'        THEN 1 END) AS views,
       COUNT(CASE WHEN event_type='addtocart'   THEN 1 END) AS carts,
       COUNT(CASE WHEN event_type='transaction' THEN 1 END) AS purchases,
       COUNT(DISTINCT transaction_id)                       AS transactions
FROM events
GROUP BY event_date;

CREATE OR REPLACE VIEW vw_hourly_pattern AS
SELECT day_of_week,
       ELT(day_of_week + 1, 'Mon','Tue','Wed','Thu','Fri','Sat','Sun') AS day_name,
       event_hour,
       COUNT(*)                                             AS total_events,
       COUNT(CASE WHEN event_type='view'        THEN 1 END) AS views,
       COUNT(CASE WHEN event_type='addtocart'   THEN 1 END) AS carts,
       COUNT(CASE WHEN event_type='transaction' THEN 1 END) AS purchases
FROM events
GROUP BY day_of_week, event_hour;

CREATE OR REPLACE VIEW vw_product_performance AS
SELECT item_id,
       COUNT(CASE WHEN event_type='view'        THEN 1 END) AS views,
       COUNT(CASE WHEN event_type='addtocart'   THEN 1 END) AS carts,
       COUNT(CASE WHEN event_type='transaction' THEN 1 END) AS purchases,
       COUNT(DISTINCT visitor_id)                           AS visitors,
       ROUND(100 * COUNT(CASE WHEN event_type='addtocart' THEN 1 END)
                 / NULLIF(COUNT(CASE WHEN event_type='view' THEN 1 END), 0), 2) AS view_to_cart_pct
FROM events
GROUP BY item_id;

CREATE OR REPLACE VIEW vw_category_performance AS
SELECT ec.category_id,
       COALESCE(CAST(ec.category_id AS CHAR), 'No category')                    AS category_label,
       c.level                                                                  AS category_level,
       c.parent_id,
       c.root_category_id,
       COALESCE(CAST(c.root_category_id AS CHAR),
                IF(ec.category_id IS NULL, 'No category', 'Hierarchy unknown')) AS top_level_label,
       COUNT(*)                                             AS total_events,
       COUNT(CASE WHEN e.event_type='view'        THEN 1 END) AS views,
       COUNT(CASE WHEN e.event_type='addtocart'   THEN 1 END) AS carts,
       COUNT(CASE WHEN e.event_type='transaction' THEN 1 END) AS purchases
FROM events e
JOIN event_category ec ON ec.event_id = e.event_id
LEFT JOIN categories c ON c.category_id = ec.category_id
GROUP BY ec.category_id, c.level, c.parent_id, c.root_category_id;

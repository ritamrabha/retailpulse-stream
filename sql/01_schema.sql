-- 01_schema.sql
-- Creates the database and all tables for the near-real-time e-commerce analytics project.
-- Run once, on an empty MySQL 8.0 server:
--     mysql -u root -p < sql/01_schema.sql        (Command Prompt)
-- or, inside the mysql client:  source sql/01_schema.sql

CREATE DATABASE IF NOT EXISTS ecommerce_bdv CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE ecommerce_bdv;

-- Every view / addtocart / transaction event. batch_id 0 = baseline, 1..N = replay batches.
CREATE TABLE IF NOT EXISTS events (
    event_id        BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_ts        BIGINT NOT NULL,              -- original timestamp in milliseconds
    event_time      DATETIME(3) NOT NULL,         -- the same moment, readable (UTC)
    visitor_id      BIGINT NOT NULL,
    event_type      ENUM('view','addtocart','transaction') NOT NULL,
    item_id         INT NOT NULL,
    transaction_id  INT NULL,                     -- filled only for transactions
    event_date      DATE NOT NULL,
    event_hour      TINYINT NOT NULL,
    day_of_week     TINYINT NOT NULL,             -- 0 = Monday ... 6 = Sunday
    batch_id        INT NOT NULL DEFAULT 0,
    INDEX idx_item      (item_id),
    INDEX idx_visitor   (visitor_id),
    INDEX idx_time      (event_time),
    INDEX idx_type      (event_type),
    INDEX idx_batch     (batch_id),
    INDEX idx_date_type (event_date, event_type)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS items (
    item_id          INT PRIMARY KEY,
    first_seen_time  DATETIME(3) NULL
) ENGINE=InnoDB;

-- Which category a product had, and from when (products can change category).
CREATE TABLE IF NOT EXISTS item_category_history (
    item_id      INT NOT NULL,
    valid_from   DATETIME NOT NULL,
    category_id  INT NOT NULL,
    PRIMARY KEY (item_id, valid_from),
    INDEX idx_category (category_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS item_availability_history (
    item_id     INT NOT NULL,
    valid_from  DATETIME NOT NULL,
    available   TINYINT NOT NULL,
    PRIMARY KEY (item_id, valid_from)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS categories (
    category_id       INT PRIMARY KEY,
    parent_id         INT NULL,
    level             TINYINT NULL,               -- 1 = top level
    root_category_id  INT NULL,                   -- the top-level category it belongs to
    INDEX idx_parent (parent_id),
    INDEX idx_root   (root_category_id)
) ENGINE=InnoDB;

-- One row per batch (0 = baseline): the plan and the progress of every stage.
CREATE TABLE IF NOT EXISTS replay_log (
    batch_id           INT PRIMARY KEY,
    week_start         DATE NULL,
    batch_start        DATETIME(3) NULL,
    batch_end          DATETIME(3) NULL,
    events_planned     INT NULL,
    events_loaded      INT NOT NULL,
    views_loaded       INT NOT NULL DEFAULT 0,
    carts_loaded       INT NOT NULL DEFAULT 0,
    purchases_loaded   INT NOT NULL DEFAULT 0,
    loaded_at          DATETIME NULL DEFAULT NULL,   -- events loaded into MySQL
    summaries_done_at  DATETIME NULL,                -- summary tables updated
    graph_done_at      DATETIME NULL                 -- Neo4j updated
) ENGINE=InnoDB;

-- The category each event had at the moment it happened.
CREATE TABLE IF NOT EXISTS event_category (
    event_id          BIGINT PRIMARY KEY,
    category_id       INT NULL,
    root_category_id  INT NULL,
    INDEX idx_ec_cat  (category_id),
    INDEX idx_ec_root (root_category_id)
) ENGINE=InnoDB;

-- Pre-counted summaries (the dashboards read these, so Tableau stays fast).
CREATE TABLE IF NOT EXISTS summary_log (
    batch_id    INT PRIMARY KEY,
    applied_at  DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS sum_daily (
    event_date DATE PRIMARY KEY, total_events INT NOT NULL, views INT NOT NULL,
    carts INT NOT NULL, purchases INT NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS sum_hourly (
    day_of_week TINYINT NOT NULL, event_hour TINYINT NOT NULL, total_events INT NOT NULL,
    views INT NOT NULL, carts INT NOT NULL, purchases INT NOT NULL,
    PRIMARY KEY (day_of_week, event_hour)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS sum_product (
    item_id INT PRIMARY KEY, views INT NOT NULL, carts INT NOT NULL, purchases INT NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS sum_category (
    category_id INT PRIMARY KEY,      -- -1 = events with no category
    total_events INT NOT NULL, views INT NOT NULL, carts INT NOT NULL, purchases INT NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS sum_visitor (
    visitor_id BIGINT PRIMARY KEY, viewed TINYINT NOT NULL, carted TINYINT NOT NULL, bought TINYINT NOT NULL
) ENGINE=InnoDB;

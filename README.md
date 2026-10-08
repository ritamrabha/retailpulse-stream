# Near-Real-Time E-Commerce Analytics with MySQL, Neo4j and Tableau

A data pipeline that takes a large historical e-commerce event log, loads the first 60% as an
initial state, and then **replays the remaining 40% in time-ordered batches**. After every batch,
MySQL, a Neo4j graph and the Tableau dashboards change, so you can watch the analytics evolve.

> This is a **simulation** of near-real-time processing on historical data. It is not a production
> streaming platform, and it contains no machine-learning model. Every number comes from counting
> the real events in the dataset.

Author: Ritam Rabha  

---

## Contents

1. [What the project demonstrates](#what-the-project-demonstrates)
2. [Architecture](#architecture)
3. [Data](#data)
4. [Repository contents](#repository-contents)
5. [Requirements](#requirements)
6. [How to run it (manual order)](#how-to-run-it-manual-order)
7. [The replay engine](#the-replay-engine)
8. [Dashboards](#dashboards)
9. [Results and measurements](#results-and-measurements)
10. [Scope and limitations](#scope-and-limitations)
11. [Status](#status)
12. [Credits and licence](#credits-and-licence)

---

## What the project demonstrates

- A reproducible path from raw CSV files to a validated relational model (MySQL) and a
  relationship model (Neo4j).
- A **chronological replay** that adds new events batch by batch, with every stage checked
  against the others (MySQL events, summary tables, graph).
- Dashboards that visibly change after each batch: KPIs, funnel, products, categories and
  graph-based relationships.
- A clear division of work: MySQL answers the relational questions (counts, funnels, rankings),
  Neo4j answers the relationship questions (who viewed or bought what, which products share
  baskets), Tableau only visualises.

## Architecture

```
 Raw CSV files (Retailrocket dataset, not in this repository)
        |
        v
 Python preparation  (01_data_preparation.ipynb)
        |   sort by time, add date fields, split 60% / 40%, number the weekly batches
        v
 +-------------------+      +--------------------+
 | MySQL             |      | Neo4j              |
 | events, items,    |      | Visitor, Product,  |
 | categories, logs, |      | Transaction,       |
 | summary tables    |      | Category nodes     |
 +---------+---------+      +----------+---------+
           |                           |
           |   replay engine (Python): one weekly batch at a time
           |   load events -> update summaries -> update graph -> check -> export
           v                           v
        dash_* views            graph CSV exports
                  \                 /
                   v               v
                       Tableau Desktop
```

## Data

The project uses the **Retailrocket recommender system dataset**, published on Kaggle:
<https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset>

- Licence of the dataset: **CC BY-NC-SA 4.0** (attribution, non-commercial, share-alike).
- It holds 2,756,101 events (2,664,312 views, 69,332 add-to-carts, 22,457 transactions) by
  1,407,580 visitors over about 4.5 months, 417,053 items with 20,275,902 property rows, and a
  category tree of 1,669 categories. All identifiers are hashed by the publisher.

**The data files are not part of this repository** (they are about 1 GB, and the dataset has its
own licence). To reproduce the project, download the dataset yourself and place these four files in
the project folder:

```
events.csv
item_properties_part1.csv
item_properties_part2.csv
category_tree.csv
```

The `.gitignore` keeps all CSV files out of git, including the files the pipeline creates from
them. The only CSVs that may be committed are the small result logs in `metrics/`.

## Repository contents

```
README.md
requirements.txt             Python packages
.env.example                 template for the Neo4j password (copy to .env, never commit .env)
docker-compose.yml           starts MySQL + Neo4j + JupyterLab (see "Run it with Docker")
Dockerfile                   the JupyterLab image
docker/03_tableau_user.sh    creates the read-only Tableau user on the first MySQL start
bdv_common.py                shared helpers + the MySQL half of the replay engine
bdv_graph.py                 the Neo4j half of the replay engine, checks, metrics, reset
01_data_preparation.ipynb    clean data, split 60/40, number the weekly batches
02_mysql.ipynb               load the baseline into MySQL, summaries, checks
03_neo4j.ipynb               load the baseline graph, graph queries and exports
04_replay_engine.ipynb       run and check the replay batches
sql/01_schema.sql            database and all tables
sql/02_views.sql             dash_* views for Tableau, vw_* views for validation
sql/03_tableau_readonly_user.sql   template for a read-only Tableau user
screenshots/                 dashboard images (baseline and after batches)
metrics/                     replay_metrics.csv and validation_results.csv (written by the engine)
```

## Requirements

- Python 3.11 or newer, and `pip install -r requirements.txt`
- MySQL 8.0
- Neo4j 2026.06 Community (Docker image `neo4j:2026.06.0-community`, or Neo4j Desktop), reachable at `bolt://localhost:7688`
  (change the address in `connect_neo4j()` if yours differs)
- Tableau Desktop for the dashboards (proprietary; screenshots are provided)
- About 16 GB of RAM was used for development (Docker limited to 7.5 GB, Neo4j heap and page cache
  of 2 GB each)

Passwords are **never** stored in the code or the notebooks. Outside Docker both engines ask for
them in a hidden prompt (`getpass`); inside Docker they come from `.env`.

## Run it with Docker (recommended)

Docker starts MySQL, Neo4j and JupyterLab together. Nothing else needs to be installed except
Docker Desktop (Windows, Mac) or Docker Engine (Linux).

**Needs:** Docker Desktop with at least **7 GB RAM** given to Docker (Settings, Resources), about
**15 GB free disk** (the data files, both databases and the prepared files), and the four dataset
CSV files (see Data).

1. Put the four dataset files in this folder, next to `docker-compose.yml`.
2. Copy `.env.example` to `.env` and set the four passwords or token inside it.
3. Open a terminal in this folder and run:
   ```
   docker compose up -d --build
   ```
   The first start downloads the images and builds the lab image (a few minutes). MySQL creates
   the database, all tables, the `dash_*` views and the read-only `tableau_ro` user by itself.
4. Wait until `docker compose ps` shows `mysql` and `neo4j` as **healthy**, then open
   <http://localhost:8888> and enter the `JUPYTER_TOKEN` from `.env`.
5. Run the notebooks in order: `01_data_preparation`, `02_mysql`, `03_neo4j`, `04_replay_engine`.
   Inside the lab the databases are found by name, so `bdv.connect_mysql()` and
   `bg.connect_neo4j()` need **no password prompt and no address** (they read them from the
   environment). If a notebook has its own connection cell with `localhost`, replace it with
   those two calls.

| What | Address from the host PC |
|---|---|
| JupyterLab | <http://localhost:8888> |
| Neo4j Browser | <http://localhost:7475> (user `neo4j`) |
| MySQL (for Tableau) | `localhost`, port **3307**, user `tableau_ro` (only the `dash_*` views) |

Stop everything with `docker compose down` (data is kept). Delete all data and start from zero with
`docker compose down -v`. If the schema files in `sql/` change, run `down -v` once, because MySQL
runs them only on the very first start.

Troubleshooting: *Neo4j restarts or the lab is killed* means Docker has too little memory; give it
more, or set `NEO4J_HEAP=1G`, `NEO4J_PAGECACHE=1G`, `MYSQL_BUFFER_POOL=512M` in `.env`. *Port already
in use* means change the port in `.env`.

## How to run it without Docker (manual order)

Run everything from the project folder. The notebooks are written to be run top to bottom.

1. **Get the data** (see above) and install the packages: `pip install -r requirements.txt`
2. **Create the MySQL schema.** Open the MySQL client in the project folder and run:
   ```
   mysql -u root -p
   mysql> source sql/01_schema.sql
   mysql> source sql/02_views.sql
   ```
3. **Prepare the data:** run `01_data_preparation.ipynb`. It writes the prepared files,
   `events_baseline.csv` (the first 60%), `events_replay_batched.csv` (the last 40% with a batch
   number) and `replay_plan.csv`. The original four CSV files are only read, never changed.
4. **Load the baseline into MySQL:** run `02_mysql.ipynb`. This loads the tables, registers the
   batches in `replay_log`, and builds the baseline summary tables
   (`bdv.apply_batch_to_summaries(engine, 0)`).
5. **Start Neo4j** (for example `docker compose up -d` after copying `.env.example` to `.env`).
6. **Load the baseline graph:** run `03_neo4j.ipynb`.
7. **Read-only Tableau user (optional):** copy `sql/03_tableau_readonly_user.sql` to
   `sql/03_tableau_readonly_user.local.sql`, put your own password in the copy, and run the copy.
   Git ignores `*.local.sql` files.
8. **Replay:** open `04_replay_engine.ipynb` and run the start-up cell, then for example:
   ```python
   import bdv_common as bdv, bdv_graph as bg
   engine = bdv.connect_mysql()
   driver = bg.connect_neo4j()

   bg.validate_graph(engine, driver, 0, label="baseline check")   # graph must agree with MySQL
   bg.run_next_full_batches(engine, driver, 1)                    # release the next weekly batch
   bdv.show_status(engine)                                        # see every batch and its status
   ```
9. **Reset to the baseline** (MySQL and the graph) to run the demo again:
   ```python
   bg.reset_everything(engine, driver, confirm=True)
   ```

## The replay engine

- **Split.** All events are sorted by timestamp. The first 60% (1,653,660 events, up to
  2015-07-21 20:32:23) is the baseline. The other 40% (1,102,441 events) is the replay.
  Events with exactly the same timestamp always stay on the same side.
- **Batch rule: one calendar week (Monday to Sunday) per batch**, 9 batches, numbered by the real
  event timestamps. Nothing is invented or redistributed. The first and the last batch are partial
  weeks.
- **One batch does, in order:** load its events into MySQL (one all-or-nothing transaction),
  update the summary tables, update the graph, compare the graph with MySQL, export the graph
  tables for Tableau, and write timings and check results to `metrics/`.
- **Safe to repeat.** A batch that is already done is skipped. A batch that stops halfway continues
  after the last finished chunk (the graph writes each chunk together with a progress marker in one
  transaction), so nothing is counted twice. Batches must run in order.
- **Checks after every batch.** Graph totals (visitors, products, transactions, views, carts,
  purchases, basket items) must equal the MySQL totals. A failed check stops the run and is logged
  in `metrics/validation_results.csv`.
- **Reset.** The replay marks everything it creates with its batch number, and saves the baseline
  numbers of every existing graph link it changes. The reset deletes the first group and restores
  the second, so no full reload is needed.
- **Why summary tables.** Counting the raw events for every dashboard query took 13 to 113 seconds.
  The dashboards read pre-counted summary tables through the `dash_*` views instead, and each batch
  only adds its own events to them, so a refresh takes about a second.

## Dashboards

| # | Dashboard | Source |
|---|---|---|
| 1 | Executive overview (KPI cards, daily trends) | `dash_kpi_overall`, `dash_daily` |
| 2 | Customer funnel (viewed, added to cart, purchased) | `dash_funnel` |
| 3 | Product performance | `dash_product` |
| 4 | Category performance | `dash_category` |
| 5 | Relationship analytics (bought together, basket sizes, audience) | graph CSV exports |
| 6 | Replay monitor | `dash_replay_monitor` (in progress) |

The Tableau workbook itself is not committed. Images are in `screenshots/`.
Conversion rates in the funnel compare counts of events (and of visitors per stage). Some buyers
have no recorded add-to-cart event, so they are indicators, not a strict funnel.

## Results and measurements

Baseline (the first 60%), verified between the source files, MySQL and Neo4j:

| | Count |
|---|---|
| Events | 1,653,660 (1,599,672 views, 40,649 add-to-carts, 13,339 purchases) |
| Visitors | 840,006 |
| Products with events | 188,915 (466,868 listed in the data) |
| Categories | 1,699 (1,669 from the tree, plus 30 used by products but missing from the tree) |
| Transactions | 10,563 (8,952 with a single item) |
| Graph | 1,041,183 nodes and 1,499,335 relationships |

Event-based rates at the baseline: view to cart 2.54%, cart to purchase 32.82%, share of visitors
who bought 0.84%.

Measured on the development machine: the MySQL part of one replay batch takes about 100 seconds
(batch 1: 99 s), and a MySQL reset takes about 4 minutes. Timings for every batch, including the
graph part, are in `metrics/replay_metrics.csv`. Every graph check is in
`metrics/validation_results.csv`.

## Scope and limitations

- A simulation on historical data, with weekly batches. It is not a live system.
- No prediction or recommendation model. The graph shows counts of what happened together, not
  recommendations, and no causal claims are made.
- 221 events (0.013%, mostly add-to-carts) are exact duplicates of other rows. They are kept,
  because their meaning cannot be proven from the data, and cart counts may be overstated by up to
  about 0.4%.
- About 9% of events belong to products with no category in the data. They appear as
  "No category".
- Products can change category over time. MySQL uses the category valid at the time of each event.
  The graph links a product to the category known when the product first appears, so the two
  category views can differ slightly.
- Category and product identifiers are hashed numbers without names.
- The Cypher statements of the replay run only against a live Neo4j. The Python logic around them
  was tested with a stand-in driver, and the MySQL side against a real MySQL 8.0 server.

## Status

- [x] Dataset study and data model
- [x] Baseline loaded and reconciled in MySQL and Neo4j
- [x] Summary tables and fast dashboard views
- [x] Dashboards 1 to 5 built on the baseline
- [x] Replay engine with checks, metrics and reset (MySQL side verified on the full data; graph side is being run batch by batch)
- [ ] Full 9-batch replay recorded in `metrics/` and `screenshots/`
- [ ] Dashboard 5 snapshot switch, and Dashboard 6 (replay monitor)
- [ ] Control panel (Streamlit) and one-command runner

## Credits and licence

- Dataset: Retailrocket recommender system dataset, published by Retail Rocket on Kaggle,
  licensed CC BY-NC-SA 4.0. No dataset files are redistributed here.
- Licence of this code: not chosen yet. Without a licence file, all rights stay with the author by
  default. Pick one (for example MIT) when you create the repository on GitHub, or add a `LICENSE`
  file later.

"""
bdv_common.py - shared helpers for the BDV e-commerce replay project (MySQL side).

Every notebook can use these with:
    import bdv_common as bdv
    engine = bdv.connect_mysql()
"""
import time
import pandas as pd
from getpass import getpass
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text

REPLAY_FILE = "events_replay_batched.csv"

# numbers we verified for the baseline (batch 0)
BASELINE = {"events": 1653660, "views": 1599672, "carts": 40649,
            "purchases": 13339, "visitors": 840006}


# ---------------------------------------------------------------- connection
def connect_mysql(user="root", host="localhost", port=3306, database="ecommerce_bdv", password=None):
    """Asks for the MySQL password in a hidden box and returns an engine."""
    if password is None:
        password = getpass("MySQL password: ")
    engine = create_engine(
        f"mysql+pymysql://{user}:{quote_plus(password)}@{host}:{port}/{database}"
    )
    with engine.connect() as conn:
        print("Connected to:", conn.execute(text("SELECT DATABASE()")).scalar())
    return engine


def q(engine, sql, params=None):
    """Runs a SELECT and returns a table."""
    with engine.connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


# ------------------------------------------------- category at the event time
def fill_event_category(engine, batch_id):
    """Stores the category each event had at the moment it happened (safe to re-run)."""
    sql = """
    INSERT INTO event_category (event_id, category_id, root_category_id)
    SELECT x.event_id, x.category_id, c.root_category_id
    FROM (SELECT e.event_id,
                 (SELECT h.category_id FROM item_category_history h
                   WHERE h.item_id = e.item_id AND h.valid_from <= e.event_time
                   ORDER BY h.valid_from DESC LIMIT 1) AS category_id
            FROM events e
            LEFT JOIN event_category ec ON ec.event_id = e.event_id
           WHERE e.batch_id = :b AND ec.event_id IS NULL) x
    LEFT JOIN categories c ON c.category_id = x.category_id
    """
    start = time.time()
    with engine.connect() as conn:
        result = conn.execute(text(sql), {"b": batch_id})
        conn.commit()
    print(f"  category lookup: {result.rowcount:,} rows added in {time.time() - start:.0f}s")


# ------------------------------------------------------------ summary tables
def apply_batch_to_summaries(engine, batch_id):
    """Adds one batch to the summary tables the dashboards read (skipped if already done)."""
    done = q(engine, "SELECT COUNT(*) AS n FROM summary_log WHERE batch_id = :b", {"b": batch_id})["n"][0]
    if done:
        print(f"batch {batch_id}: already in the summaries. Skipped.")
        return

    if batch_id > 0:
        n = q(engine, "SELECT COUNT(*) AS n FROM events WHERE batch_id = :b", {"b": batch_id})["n"][0]
        if n == 0:
            raise RuntimeError(f"Batch {batch_id} has no events in MySQL yet. Load it first.")

    fill_event_category(engine, batch_id)

    steps = {
    "daily": """
        INSERT INTO sum_daily (event_date, total_events, views, carts, purchases)
        SELECT event_date, COUNT(*), SUM(event_type='view'), SUM(event_type='addtocart'), SUM(event_type='transaction')
        FROM events WHERE batch_id = :b GROUP BY event_date
        ON DUPLICATE KEY UPDATE total_events = total_events + VALUES(total_events),
            views = views + VALUES(views), carts = carts + VALUES(carts), purchases = purchases + VALUES(purchases)""",
    "hourly": """
        INSERT INTO sum_hourly (day_of_week, event_hour, total_events, views, carts, purchases)
        SELECT day_of_week, event_hour, COUNT(*), SUM(event_type='view'), SUM(event_type='addtocart'), SUM(event_type='transaction')
        FROM events WHERE batch_id = :b GROUP BY day_of_week, event_hour
        ON DUPLICATE KEY UPDATE total_events = total_events + VALUES(total_events),
            views = views + VALUES(views), carts = carts + VALUES(carts), purchases = purchases + VALUES(purchases)""",
    "product": """
        INSERT INTO sum_product (item_id, views, carts, purchases)
        SELECT item_id, SUM(event_type='view'), SUM(event_type='addtocart'), SUM(event_type='transaction')
        FROM events WHERE batch_id = :b GROUP BY item_id
        ON DUPLICATE KEY UPDATE views = views + VALUES(views), carts = carts + VALUES(carts),
            purchases = purchases + VALUES(purchases)""",
    "category": """
        INSERT INTO sum_category (category_id, total_events, views, carts, purchases)
        SELECT COALESCE(ec.category_id, -1), COUNT(*), SUM(e.event_type='view'),
               SUM(e.event_type='addtocart'), SUM(e.event_type='transaction')
        FROM events e JOIN event_category ec ON ec.event_id = e.event_id
        WHERE e.batch_id = :b GROUP BY COALESCE(ec.category_id, -1)
        ON DUPLICATE KEY UPDATE total_events = total_events + VALUES(total_events),
            views = views + VALUES(views), carts = carts + VALUES(carts), purchases = purchases + VALUES(purchases)""",
    "visitor": """
        INSERT INTO sum_visitor (visitor_id, viewed, carted, bought)
        SELECT visitor_id, MAX(event_type='view'), MAX(event_type='addtocart'), MAX(event_type='transaction')
        FROM events WHERE batch_id = :b GROUP BY visitor_id
        ON DUPLICATE KEY UPDATE viewed = GREATEST(viewed, VALUES(viewed)),
            carted = GREATEST(carted, VALUES(carted)), bought = GREATEST(bought, VALUES(bought))""",
    }

    # one all-or-nothing transaction: either every summary is updated, or none is
    with engine.begin() as conn:
        for name, sql in steps.items():
            start = time.time()
            conn.execute(text(sql), {"b": batch_id})
            print(f"  summary {name:<9} done in {time.time() - start:.0f}s")
        conn.execute(text("INSERT INTO summary_log (batch_id) VALUES (:b)"), {"b": batch_id})
        conn.execute(text("UPDATE replay_log SET summaries_done_at = NOW() WHERE batch_id = :b"), {"b": batch_id})
    print(f"batch {batch_id}: added to the summaries.")


# ------------------------------------------------------- load events (MySQL)
def load_batch_to_mysql(engine, batch_id, csv=REPLAY_FILE):
    """Loads one replay batch into the events table, all or nothing (skipped if already loaded)."""
    if batch_id < 1:
        raise ValueError("Replay batches are numbered from 1. Batch 0 is the baseline.")

    log = q(engine, "SELECT events_planned, loaded_at FROM replay_log WHERE batch_id = :b", {"b": batch_id})
    if log.empty:
        raise RuntimeError(f"Batch {batch_id} is not in replay_log.")
    planned = int(log["events_planned"][0])

    if pd.notna(log["loaded_at"][0]):
        print(f"batch {batch_id}: already loaded into MySQL. Skipped.")
        return

    not_loaded_before = q(engine, """SELECT COUNT(*) AS n FROM replay_log
                                     WHERE batch_id > 0 AND batch_id < :b AND loaded_at IS NULL""",
                          {"b": batch_id})["n"][0]
    if not_loaded_before:
        raise RuntimeError(f"Earlier batches are not loaded yet. Load batches in order (1, 2, 3 ...).")

    leftovers = q(engine, "SELECT COUNT(*) AS n FROM events WHERE batch_id = :b", {"b": batch_id})["n"][0]
    if leftovers:
        raise RuntimeError(f"Batch {batch_id} already has {leftovers} rows in events but is not logged. "
                           f"Run reset_mysql_to_baseline(engine, confirm=True) first.")

    start = time.time()
    parts = []
    for chunk in pd.read_csv(csv, chunksize=500_000, parse_dates=["event_time", "event_date"],
                             dtype={"transaction_id": "Int64"}):
        parts.append(chunk[chunk["batch_id"] == batch_id])
    df = pd.concat(parts, ignore_index=True)
    df["event_date"] = df["event_date"].dt.date

    if len(df) != planned:
        raise RuntimeError(f"The file has {len(df)} rows for batch {batch_id}, but the plan says {planned}.")

    views = int((df["event_type"] == "view").sum())
    carts = int((df["event_type"] == "addtocart").sum())
    purchases = int((df["event_type"] == "transaction").sum())

    # one transaction: if anything fails, nothing is saved
    with engine.begin() as conn:
        df.to_sql("events", conn, if_exists="append", index=False, chunksize=20_000, method="multi")
        loaded = conn.execute(text("SELECT COUNT(*) FROM events WHERE batch_id = :b"), {"b": batch_id}).scalar()
        if loaded != planned:
            raise RuntimeError(f"Loaded {loaded} rows but expected {planned}. Nothing was saved.")
        conn.execute(text("""UPDATE replay_log
                             SET events_loaded = :n, views_loaded = :v, carts_loaded = :c,
                                 purchases_loaded = :p, loaded_at = NOW()
                             WHERE batch_id = :b"""),
                     {"n": loaded, "v": views, "c": carts, "p": purchases, "b": batch_id})
    print(f"batch {batch_id}: {planned:,} events loaded into MySQL in {time.time() - start:.0f}s "
          f"({views:,} views, {carts:,} carts, {purchases:,} purchases)")


# -------------------------------------------------------------- run a batch
def run_batch(engine, batch_id):
    """MySQL part of one batch: events, category lookup, summaries, log. (The graph is added later.)"""
    start = time.time()
    print(f"=== Batch {batch_id} ===")
    load_batch_to_mysql(engine, batch_id)
    apply_batch_to_summaries(engine, batch_id)
    print(f"=== Batch {batch_id} finished in {time.time() - start:.0f}s ===\n")


def next_pending_batch(engine):
    """The lowest batch number that is not finished yet (None if all are done)."""
    r = q(engine, "SELECT MIN(batch_id) AS b FROM replay_log WHERE batch_id > 0 AND summaries_done_at IS NULL")["b"][0]
    return None if pd.isna(r) else int(r)


def run_next_batches(engine, n=1):
    """Runs the next n waiting batches, in order."""
    for _ in range(n):
        b = next_pending_batch(engine)
        if b is None:
            print("All batches are done.")
            return
        run_batch(engine, b)


# ------------------------------------------------------------- look at state
def show_status(engine):
    return q(engine, """SELECT batch_id, batch_label, events_planned, events_loaded,
                               loaded_at, summaries_done_at, graph_done_at, status
                        FROM dash_replay_monitor ORDER BY batch_id""")


def show_kpis(engine):
    return q(engine, """SELECT visitors, views, carts, purchases, transactions,
                               view_to_cart_pct, cart_to_purchase_pct, data_up_to, latest_batch
                        FROM dash_kpi_overall""")


# -------------------------------------------------------------------- reset
def _delete_in_chunks(engine, sql, params=None, label=""):
    total = 0
    with engine.connect() as conn:
        while True:
            n = conn.execute(text(sql), params or {}).rowcount
            conn.commit()
            total += n
            if n == 0:
                break
    print(f"  removed {total:,} {label}")


def reset_mysql_to_baseline(engine, confirm=False, expected=None):
    """Removes all replay batches from MySQL and rebuilds the summaries from the baseline."""
    expected = expected or BASELINE
    if not confirm:
        print("This removes every replay batch from MySQL and rebuilds the summaries.\n"
              "Run again with confirm=True if you are sure.")
        return
    start = time.time()
    print("Resetting MySQL to the baseline ...")

    _delete_in_chunks(engine, "DELETE FROM events WHERE batch_id > 0 LIMIT 100000", label="replay events")
    base_max = q(engine, "SELECT MAX(event_id) AS m FROM events")["m"][0]
    _delete_in_chunks(engine, "DELETE FROM event_category WHERE event_id > :m LIMIT 100000",
                      {"m": int(base_max)}, label="category lookup rows")

    with engine.connect() as conn:
        for t in ["sum_daily", "sum_hourly", "sum_product", "sum_category", "sum_visitor", "summary_log"]:
            conn.execute(text(f"TRUNCATE TABLE {t}"))
        conn.execute(text("""UPDATE replay_log
                             SET events_loaded = 0, views_loaded = 0, carts_loaded = 0, purchases_loaded = 0,
                                 loaded_at = NULL, summaries_done_at = NULL, graph_done_at = NULL
                             WHERE batch_id > 0"""))
        conn.commit()

    apply_batch_to_summaries(engine, 0)

    k = show_kpis(engine).iloc[0]
    got = {"events": int(q(engine, "SELECT COUNT(*) AS n FROM events")["n"][0]),
           "views": int(k["views"]), "carts": int(k["carts"]),
           "purchases": int(k["purchases"]), "visitors": int(k["visitors"])}
    ok = all(got[key] == expected[key] for key in expected)
    print("Baseline numbers after reset:", got)
    print("RESET OK - matches the baseline." if ok else f"MISMATCH - expected {expected}")
    print(f"Reset finished in {time.time() - start:.0f}s")

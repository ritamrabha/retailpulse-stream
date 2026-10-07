"""
bdv_graph.py - graph (Neo4j) half of the BDV replay engine.

Use it together with bdv_common.py:
    import bdv_common as bdv
    import bdv_graph as bg
    engine = bdv.connect_mysql()
    driver = bg.connect_neo4j()
    bg.run_full_batch(engine, driver, 1)

How a batch stays safe:
  * Each chunk of rows is written to Neo4j in ONE transaction together with a progress marker
    (a ReplayBatch node). If something stops halfway, the next run continues after the last
    finished chunk, so nothing is counted twice.
  * Everything the replay creates carries a "created_batch" property, and every baseline
    relationship that the replay changes first stores its baseline numbers (base_count ...).
    That is what makes the reset fast.
"""
import os
import time
import pandas as pd
from getpass import getpass
from neo4j import GraphDatabase
from sqlalchemy import text

import bdv_common as bdv

METRICS_DIR = "metrics"
EXPORT_DIR = "tableau_exports"
NODE_CHUNK = 10_000
REL_CHUNK = 5_000


# ---------------------------------------------------------------- connection
def connect_neo4j(uri=None, user=None, password=None):
    """Returns a Neo4j driver.
    Settings come from the arguments, else from the environment (used inside Docker:
    BDV_NEO4J_URI, BDV_NEO4J_USER, BDV_NEO4J_PASSWORD), else the local defaults
    (bolt://localhost:7688, neo4j). Only if no password is known, a hidden box asks for it."""
    uri = uri or os.environ.get("BDV_NEO4J_URI", "bolt://localhost:7688")
    user = user or os.environ.get("BDV_NEO4J_USER", "neo4j")
    if password is None:
        password = os.environ.get("BDV_NEO4J_PASSWORD")
    if password is None:
        password = getpass("Neo4j password: ")
    driver = GraphDatabase.driver(uri, auth=(user, password))
    driver.verify_connectivity()
    with driver.session(database="neo4j") as s:
        s.run("CREATE CONSTRAINT replaybatch_id_unique IF NOT EXISTS "
              "FOR (b:ReplayBatch) REQUIRE b.batch_id IS UNIQUE").consume()
        n = s.run("MATCH (n) RETURN count(n) AS n").single()["n"]
    print(f"Connected to Neo4j ({uri}). Nodes in the graph: {n:,}")
    return driver


def _rows(df):
    """Plain Python values (None for empty) so the Neo4j driver accepts them."""
    df = df.astype(object).where(df.notna(), None)
    return df.to_dict("records")


# ------------------------------------------------------------ the Cypher text
C_VISITORS = """UNWIND $rows AS r
MERGE (v:Visitor {visitor_id: r.visitor_id})
ON CREATE SET v.created_batch = $b"""

C_PRODUCTS = """UNWIND $rows AS r
MERGE (p:Product {item_id: r.item_id})
ON CREATE SET p.created_batch = $b"""

# a product gets its category link once, when it first appears
C_BELONGS = """UNWIND $rows AS r
MATCH (p:Product {item_id: r.item_id}) WHERE p.created_batch = $b
MATCH (c:Category {category_id: r.category_id})
MERGE (p)-[:BELONGS_TO]->(c)"""

C_TRANSACTIONS = """UNWIND $rows AS r
MERGE (t:Transaction {transaction_id: r.transaction_id})
ON CREATE SET t.first_ts = r.first_ts, t.created_batch = $b"""

C_MADE = """UNWIND $rows AS r
MATCH (v:Visitor {visitor_id: r.visitor_id})
MATCH (t:Transaction {transaction_id: r.transaction_id})
MERGE (v)-[m:MADE]->(t)
ON CREATE SET m.created_batch = $b"""

C_CONTAINS = """UNWIND $rows AS r
MATCH (t:Transaction {transaction_id: r.transaction_id})
MATCH (p:Product {item_id: r.item_id})
MERGE (t)-[x:CONTAINS]->(p)
ON CREATE SET x.quantity = r.quantity, x.created_batch = $b
ON MATCH SET x.base_quantity = coalesce(x.base_quantity, x.quantity),
             x.quantity = x.quantity + r.quantity"""


def _c_interaction(rel):
    return f"""UNWIND $rows AS r
MATCH (v:Visitor {{visitor_id: r.visitor_id}})
MATCH (p:Product {{item_id: r.item_id}})
MERGE (v)-[x:{rel}]->(p)
ON CREATE SET x.count = r.cnt, x.first_ts = r.first_ts, x.last_ts = r.last_ts, x.created_batch = $b
ON MATCH SET x.base_count = coalesce(x.base_count, x.count),
             x.base_last_ts = coalesce(x.base_last_ts, x.last_ts),
             x.count = x.count + r.cnt,
             x.last_ts = CASE WHEN r.last_ts > x.last_ts THEN r.last_ts ELSE x.last_ts END"""


# ------------------------------------------------------------ progress marker
def _get_progress(driver, batch_id):
    with driver.session(database="neo4j") as s:
        rec = s.run("MERGE (b:ReplayBatch {batch_id: $b}) "
                    "ON CREATE SET b.started_at = toString(datetime()) "
                    "RETURN properties(b) AS p", b=batch_id).single()
    return dict(rec["p"])


def _chunk_tx(tx, cypher, rows, batch_id, props):
    # the rows and the progress marker are saved together, or not at all
    tx.run(cypher, rows=rows, b=batch_id).consume()
    tx.run("MATCH (s:ReplayBatch {batch_id: $b}) SET s += $props", b=batch_id, props=props).consume()


def _run_stage(driver, progress, batch_id, stage, cypher, df, chunk):
    rows = _rows(df)
    total = len(rows)
    done = int(progress.get(stage, 0))
    if total == 0:
        print(f"  {stage:<14} nothing to do")
        return 0.0
    if done >= total:
        print(f"  {stage:<14} already done ({total:,} rows)")
        return 0.0
    start = time.time()
    with driver.session(database="neo4j") as s:
        for i in range(done, total, chunk):
            upto = min(i + chunk, total)
            s.execute_write(_chunk_tx, cypher, rows[i:upto], batch_id, {stage: upto})
    progress[stage] = total
    seconds = time.time() - start
    print(f"  {stage:<14} {total:,} rows in {seconds:.0f}s")
    return seconds


# --------------------------------------------------------- one batch -> graph
def apply_batch_to_graph(engine, driver, batch_id):
    """Adds one batch to the graph (skipped if already done). Returns seconds per stage."""
    if batch_id < 1:
        raise ValueError("Replay batches are numbered from 1.")
    log = bdv.q(engine, "SELECT loaded_at, graph_done_at, batch_end FROM replay_log WHERE batch_id = :b",
                {"b": batch_id})
    if log.empty:
        raise RuntimeError(f"Batch {batch_id} is not in replay_log.")
    if pd.isna(log["loaded_at"][0]):
        raise RuntimeError(f"Batch {batch_id} is not in MySQL yet. Run the MySQL part first.")
    if pd.notna(log["graph_done_at"][0]):
        print(f"batch {batch_id}: already in the graph. Skipped.")
        return {}
    earlier = bdv.q(engine, """SELECT COUNT(*) AS n FROM replay_log
                               WHERE batch_id > 0 AND batch_id < :b AND graph_done_at IS NULL""",
                    {"b": batch_id})["n"][0]
    if earlier:
        raise RuntimeError("Earlier batches are not in the graph yet. Run batches in order.")

    cutoff = log["batch_end"][0].to_pydatetime()      # categories are taken as of the end of the batch
    b = {"b": batch_id}
    times = {}

    visitors = bdv.q(engine, "SELECT DISTINCT visitor_id FROM events WHERE batch_id = :b ORDER BY visitor_id", b)
    products = bdv.q(engine, """
        SELECT p.item_id, h.category_id
        FROM (SELECT DISTINCT item_id FROM events WHERE batch_id = :b) p
        LEFT JOIN item_category_history h
          ON h.item_id = p.item_id
         AND h.valid_from = (SELECT MAX(h2.valid_from) FROM item_category_history h2
                              WHERE h2.item_id = p.item_id AND h2.valid_from <= :cut)
        ORDER BY p.item_id""", {"b": batch_id, "cut": cutoff})
    products["category_id"] = products["category_id"].astype("Int64")

    def pairs(event_type):
        return bdv.q(engine, """
            SELECT visitor_id, item_id, COUNT(*) AS cnt, MIN(event_ts) AS first_ts, MAX(event_ts) AS last_ts
            FROM events WHERE batch_id = :b AND event_type = :t
            GROUP BY visitor_id, item_id ORDER BY visitor_id, item_id""", {"b": batch_id, "t": event_type})

    tx_where = "batch_id = :b AND event_type = 'transaction' AND transaction_id IS NOT NULL"
    transactions = bdv.q(engine, f"""SELECT transaction_id, MIN(event_ts) AS first_ts FROM events
                                     WHERE {tx_where} GROUP BY transaction_id ORDER BY transaction_id""", b)
    made = bdv.q(engine, f"""SELECT DISTINCT transaction_id, visitor_id FROM events
                             WHERE {tx_where} ORDER BY transaction_id, visitor_id""", b)
    contains = bdv.q(engine, f"""SELECT transaction_id, item_id, COUNT(*) AS quantity FROM events
                                 WHERE {tx_where} GROUP BY transaction_id, item_id
                                 ORDER BY transaction_id, item_id""", b)

    progress = _get_progress(driver, batch_id)
    stages = [
        ("visitors",      C_VISITORS,                       visitors,                                         NODE_CHUNK),
        ("products",      C_PRODUCTS,                       products[["item_id"]],                            NODE_CHUNK),
        ("belongs_to",    C_BELONGS,                        products[products["category_id"].notna()],        REL_CHUNK),
        ("transactions",  C_TRANSACTIONS,                   transactions,                                     NODE_CHUNK),
        ("viewed",        _c_interaction("VIEWED"),         pairs("view"),                                    REL_CHUNK),
        ("added_to_cart", _c_interaction("ADDED_TO_CART"),  pairs("addtocart"),                               REL_CHUNK),
        ("purchased",     _c_interaction("PURCHASED"),      pairs("transaction"),                             REL_CHUNK),
        ("made",          C_MADE,                           made,                                             REL_CHUNK),
        ("contains",      C_CONTAINS,                       contains,                                         REL_CHUNK),
    ]
    for stage, cypher, df, chunk in stages:
        times[stage] = _run_stage(driver, progress, batch_id, stage, cypher, df, chunk)

    with driver.session(database="neo4j") as s:
        s.run("MATCH (s:ReplayBatch {batch_id: $b}) SET s.finished_at = toString(datetime())", b=batch_id).consume()
    with engine.begin() as conn:
        conn.execute(text("UPDATE replay_log SET graph_done_at = NOW() WHERE batch_id = :b"), {"b": batch_id})
    print(f"batch {batch_id}: added to the graph.")
    return times


# ------------------------------------------------------------ check the graph
def graph_counts(driver):
    """What the graph holds right now."""
    queries = {
        "visitors":     "MATCH (v:Visitor) RETURN count(v) AS n",
        "products":     "MATCH (p:Product) RETURN count(p) AS n",
        "transactions": "MATCH (t:Transaction) RETURN count(t) AS n",
        "views":        "MATCH ()-[x:VIEWED]->() RETURN coalesce(sum(x.count), 0) AS n",
        "carts":        "MATCH ()-[x:ADDED_TO_CART]->() RETURN coalesce(sum(x.count), 0) AS n",
        "purchases":    "MATCH ()-[x:PURCHASED]->() RETURN coalesce(sum(x.count), 0) AS n",
        "contains_qty": "MATCH ()-[x:CONTAINS]->() RETURN coalesce(sum(x.quantity), 0) AS n",
    }
    out = {}
    with driver.session(database="neo4j") as s:
        for name, cy in queries.items():
            out[name] = int(s.run(cy).single()["n"])
    return out


def mysql_expected(engine):
    """What the graph should hold, according to MySQL."""
    a = bdv.q(engine, """SELECT SUM(event_type = 'view') AS v, SUM(event_type = 'addtocart') AS c,
                                SUM(event_type = 'transaction') AS p,
                                SUM(event_type = 'transaction' AND transaction_id IS NOT NULL) AS pq,
                                COUNT(DISTINCT CASE WHEN event_type = 'transaction' THEN transaction_id END) AS tx
                         FROM events""").iloc[0]
    return {
        "visitors":     int(bdv.q(engine, "SELECT COUNT(*) AS n FROM sum_visitor")["n"][0]),
        "products":     int(bdv.q(engine, "SELECT COUNT(*) AS n FROM sum_product")["n"][0]),
        "transactions": int(a["tx"]),
        "views":        int(a["v"]),
        "carts":        int(a["c"]),
        "purchases":    int(a["p"]),
        "contains_qty": int(a["pq"]),
    }


def _append_csv(path, df):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, mode="a", header=not os.path.exists(path), index=False)


def validate_graph(engine, driver, batch_id, label=None):
    """Compares graph totals with MySQL totals, prints the result and logs it. True if all match."""
    label = label or f"batch {batch_id}"
    start = time.time()
    exp, got = mysql_expected(engine), graph_counts(driver)
    rows = [{"checked_at": time.strftime("%Y-%m-%d %H:%M:%S"), "batch_id": batch_id, "label": label,
             "check": name, "expected_mysql": exp[name], "actual_graph": got[name],
             "passed": exp[name] == got[name]} for name in exp]
    result = pd.DataFrame(rows)
    _append_csv(os.path.join(METRICS_DIR, "validation_results.csv"), result)
    print(result[["check", "expected_mysql", "actual_graph", "passed"]].to_string(index=False))
    ok = bool(result["passed"].all())
    print(("ALL GRAPH CHECKS PASSED" if ok else "GRAPH CHECK FAILED - do not trust the graph numbers")
          + f"  ({time.time() - start:.0f}s)")
    return ok


# ----------------------------------------------------------- export for Tableau
def _cypher_table(driver, query, **params):
    with driver.session(database="neo4j") as session:
        return pd.DataFrame(session.run(query, **params).data())


def export_graph_tables(driver, snapshot):
    """Writes the four graph tables Tableau reads, labelled with the snapshot name."""
    os.makedirs(EXPORT_DIR, exist_ok=True)
    out = {}
    out["bought_together"] = _cypher_table(driver, """
        MATCH (p1:Product)<-[:CONTAINS]-(t:Transaction)-[:CONTAINS]->(p2:Product)
        WHERE p1.item_id < p2.item_id
        WITH p1, p2, count(t) AS bought_together
        WHERE bought_together >= 2
        RETURN p1.item_id AS product_1, p2.item_id AS product_2, bought_together
        ORDER BY bought_together DESC LIMIT 2000""")

    baskets = _cypher_table(driver, """
        MATCH (t:Transaction)-[c:CONTAINS]->(:Product)
        WITH t, sum(c.quantity) AS items
        RETURN items AS items_in_basket, count(*) AS baskets
        ORDER BY items_in_basket""")
    baskets["size_group"] = baskets["items_in_basket"].apply(lambda n: str(n) if n <= 5 else "6+")
    baskets = baskets.groupby("size_group", as_index=False)["baskets"].sum()
    baskets["sort_order"] = baskets["size_group"].apply(lambda s: 6 if s == "6+" else int(s))
    out["basket_sizes"] = baskets.sort_values("sort_order").reset_index(drop=True)

    audience = _cypher_table(driver, """
        MATCH (v:Visitor)-[x:VIEWED]->(p:Product)
        WITH p, count(v) AS distinct_viewers, sum(x.count) AS total_views
        ORDER BY distinct_viewers DESC LIMIT 500
        RETURN p.item_id AS item_id, distinct_viewers, total_views""")
    audience["views_per_viewer"] = (audience["total_views"] / audience["distinct_viewers"]).round(2)
    out["product_audience"] = audience

    out["products_per_top_category"] = _cypher_table(driver, """
        MATCH (root:Category) WHERE NOT EXISTS { (root)-[:CHILD_OF]->() }
        MATCH (p:Product)-[:BELONGS_TO]->(:Category)-[:CHILD_OF*0..]->(root)
        RETURN root.category_id AS top_level_category, count(DISTINCT p) AS products
        ORDER BY products DESC""")

    for name, df in out.items():
        df.insert(0, "snapshot", snapshot)
        df.to_csv(os.path.join(EXPORT_DIR, f"{name}_{snapshot}.csv"), index=False)
        print(f"  saved {EXPORT_DIR}/{name}_{snapshot}.csv ({len(df)} rows)")
    return out


# ------------------------------------------------------------- run batches
def run_full_batch(engine, driver, batch_id, export=True):
    """MySQL + summaries + graph + checks + metrics (+ graph CSV export for Tableau)."""
    t_all = time.time()
    already_done = bool(bdv.q(engine, "SELECT COUNT(*) AS n FROM replay_log WHERE batch_id = :b AND graph_done_at IS NOT NULL",
                              {"b": batch_id})["n"][0])
    print(f"########## Batch {batch_id}: MySQL ##########")
    t = time.time(); bdv.load_batch_to_mysql(engine, batch_id);       s_load = time.time() - t
    t = time.time(); bdv.apply_batch_to_summaries(engine, batch_id);  s_sum = time.time() - t
    print(f"########## Batch {batch_id}: graph ##########")
    t = time.time(); apply_batch_to_graph(engine, driver, batch_id);  s_graph = time.time() - t
    print(f"########## Batch {batch_id}: checks ##########")
    t = time.time(); ok = validate_graph(engine, driver, batch_id);   s_check = time.time() - t

    s_export = 0.0
    if export and ok:
        print(f"########## Batch {batch_id}: export for Tableau ##########")
        t = time.time(); export_graph_tables(driver, f"after_batch_{batch_id}"); s_export = time.time() - t
    elif export:
        print("Export skipped because the graph check failed.")

    total = time.time() - t_all
    if not already_done:                   # a batch that was skipped is not logged again
        info = bdv.q(engine, "SELECT events_loaded FROM replay_log WHERE batch_id = :b", {"b": batch_id})
        _append_csv(os.path.join(METRICS_DIR, "replay_metrics.csv"), pd.DataFrame([{
            "finished_at": time.strftime("%Y-%m-%d %H:%M:%S"), "batch_id": batch_id,
            "events": int(info["events_loaded"][0]),
            "seconds_mysql_load": round(s_load, 1), "seconds_summaries": round(s_sum, 1),
            "seconds_graph": round(s_graph, 1), "seconds_checks": round(s_check, 1),
            "seconds_export": round(s_export, 1), "seconds_total": round(total, 1),
            "graph_check_passed": ok}]))
    print(f"########## Batch {batch_id} finished in {total:.0f}s ##########\n")
    return ok


def next_pending_full_batch(engine):
    r = bdv.q(engine, "SELECT MIN(batch_id) AS b FROM replay_log WHERE batch_id > 0 AND graph_done_at IS NULL")["b"][0]
    return None if pd.isna(r) else int(r)


def run_next_full_batches(engine, driver, n=1, export=True):
    """Runs the next n waiting batches in order. Stops if a graph check fails."""
    for _ in range(n):
        b = next_pending_full_batch(engine)
        if b is None:
            print("All batches are done.")
            return
        if not run_full_batch(engine, driver, b, export=export):
            print("Stopped: fix or reset before running more batches.")
            return


# -------------------------------------------------------------------- reset
def _loop_until_zero(driver, cypher, label):
    total = 0
    with driver.session(database="neo4j") as s:
        while True:
            n = s.run(cypher).single()["n"]
            total += n
            if n == 0:
                break
    print(f"  {label}: {total:,}")


def reset_graph_to_baseline(driver, confirm=False):
    """Removes everything the replay added to the graph and restores the baseline numbers."""
    if not confirm:
        print("This removes everything the replay added to the graph. Run again with confirm=True.")
        return
    start = time.time()
    print("Resetting the graph to the baseline ...")
    for rel in ["VIEWED", "ADDED_TO_CART", "PURCHASED", "MADE", "CONTAINS"]:
        _loop_until_zero(driver, f"MATCH ()-[x:{rel}]->() WHERE x.created_batch IS NOT NULL "
                                 f"WITH x LIMIT 50000 DELETE x RETURN count(*) AS n", f"{rel} links removed")
    for label in ["Transaction", "Visitor", "Product"]:
        _loop_until_zero(driver, f"MATCH (n:{label}) WHERE n.created_batch IS NOT NULL "
                                 f"WITH n LIMIT 20000 DETACH DELETE n RETURN count(*) AS n", f"{label} nodes removed")
    for rel in ["VIEWED", "ADDED_TO_CART", "PURCHASED"]:
        _loop_until_zero(driver, f"MATCH ()-[x:{rel}]->() WHERE x.base_count IS NOT NULL WITH x LIMIT 50000 "
                                 f"SET x.count = x.base_count, x.last_ts = x.base_last_ts "
                                 f"REMOVE x.base_count, x.base_last_ts RETURN count(*) AS n",
                         f"{rel} counts restored")
    _loop_until_zero(driver, "MATCH ()-[x:CONTAINS]->() WHERE x.base_quantity IS NOT NULL WITH x LIMIT 50000 "
                             "SET x.quantity = x.base_quantity REMOVE x.base_quantity RETURN count(*) AS n",
                     "CONTAINS quantities restored")
    _loop_until_zero(driver, "MATCH (b:ReplayBatch) WITH b LIMIT 1000 DETACH DELETE b RETURN count(*) AS n",
                     "batch markers removed")
    print(f"Graph reset finished in {time.time() - start:.0f}s")


def reset_everything(engine, driver, confirm=False):
    """Graph first, then MySQL, then a check that both are back at the baseline."""
    if not confirm:
        print("This returns MySQL AND the graph to the baseline. Run again with confirm=True.")
        return
    reset_graph_to_baseline(driver, confirm=True)
    bdv.reset_mysql_to_baseline(engine, confirm=True)
    print("Checking the graph against MySQL ...")
    validate_graph(engine, driver, 0, label="after reset")

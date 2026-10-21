"""Exercise baseline and cross-database increments in the local demo only."""

import json
import os
import time
import urllib.error
import urllib.request

import psycopg2

BASE_URL = "http://127.0.0.1:8180"
JOB_ID = "job_demo_book_catalog"


def get_json(path):
    with urllib.request.urlopen(BASE_URL + path, timeout=10) as response:
        return json.load(response)["data"]


def document():
    with urllib.request.urlopen("http://elasticsearch:9200/book_catalog_v1/_doc/1", timeout=10) as response:
        return json.load(response)["_source"]


def wait_for(predicate, label, timeout=120):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        runtime = get_json(f"/api/jobs/{JOB_ID}/runtime")
        if runtime.get("status") == "failed":
            raise AssertionError(f"Demo job failed during {label}")
        try:
            if predicate():
                print(f"PASS {label}", flush=True)
                return
        except urllib.error.HTTPError as exc:
            if exc.code != 404:
                raise
        time.sleep(1)
    raise AssertionError(f"Timed out waiting for {label}")


def main():
    if os.environ.get("PGSYNC_STUDIO_PROFILE") != "dev":
        raise SystemExit("This check is only for the disposable local Compose demo.")
    boot = get_json("/api/bootstrap")
    demo = next((j for j in boot["jobs"] if j["id"] == JOB_ID), None)
    if not demo or demo["index_name"] != "book_catalog_v1":
        raise SystemExit("Initialize the demo job first.")
    sources = {c["id"]: c for c in boot["connections"]}
    for table in demo["tables"]:
        c = sources[table["connection_id"]]
        if c["host"] != "postgres" or c["database"] not in {"catalog", "analytics"}:
            raise SystemExit("The demo must use only its Compose databases.")

    wait_for(lambda: (document().get("author") or {}).get("name") == "Demo Author"
             and isinstance((document().get("book_metrics") or {}).get("views"), int), "baseline assembly")
    password = os.environ["PGSYNC_STUDIO_PG_PASSWORD"]
    with psycopg2.connect(host="postgres", user="studio", password=password, dbname="analytics") as conn:
        with conn.cursor() as cursor:
            cursor.execute("UPDATE book_metrics SET views=views+1, updated_at=now() WHERE book_id=1 RETURNING views")
            expected = cursor.fetchone()[0]
    wait_for(lambda: document()["book_metrics"]["views"] == expected, "cross-database metric update")

    title = f"Updated demo title {time.time_ns()}"
    with psycopg2.connect(host="postgres", user="studio", password=password, dbname="catalog") as conn:
        with conn.cursor() as cursor:
            cursor.execute("UPDATE books SET title=%s, updated_at=now() WHERE id=1", (title,))
    wait_for(lambda: document()["title"] == title, "root-table update")
    debug = get_json(f"/api/jobs/{JOB_ID}/debug")
    assert debug["runner"]["thread"]["alive"]
    assert all(x["alive"] for x in debug["runner"]["native_listener_threads"].values())
    print("PASS actual runner and listener threads")


if __name__ == "__main__":
    main()

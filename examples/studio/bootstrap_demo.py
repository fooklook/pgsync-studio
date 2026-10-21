"""Create the synthetic Compose demo job through the Studio API."""

import argparse
import json
import urllib.error
import urllib.request

DEMO_JOB_ID = "job_demo_book_catalog"


def api(base_url, path, payload=None):
    request = urllib.request.Request(
        base_url.rstrip("/") + path,
        data=json.dumps(payload).encode() if payload is not None else None,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            result = json.load(response)
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Studio API {path} failed with HTTP {exc.code}") from exc
    if not result.get("ok"):
        raise RuntimeError(f"Studio API {path} rejected the request")
    return result.get("data")


def demo_job():
    return {
        "id": DEMO_JOB_ID,
        "name": "book-catalog-demo",
        "index_name": "book_catalog_v1",
        "root_table_id": "table_demo_books",
        "enable_durable_change_events": True,
        "enable_assembly_fallback": False,
        "page_size": 100,
        "incremental_batch_size": 100,
        "poll_interval_seconds": 60,
        "full_resync_interval_seconds": 1800,
        "tables": [
            {
                "id": "table_demo_books", "connection_id": "conn_demo_catalog",
                "schema": "public", "table": "books", "alias": "books",
                "primary_key": ["id"], "cursor_column": "updated_at",
                "soft_delete_column": "deleted",
            },
            {
                "id": "table_demo_authors", "connection_id": "conn_demo_catalog",
                "schema": "public", "table": "authors", "alias": "author",
                "primary_key": ["id"], "cursor_column": "updated_at",
                "soft_delete_column": "",
            },
            {
                "id": "table_demo_metrics", "connection_id": "conn_demo_analytics",
                "schema": "public", "table": "book_metrics", "alias": "book_metrics",
                "primary_key": ["book_id"], "cursor_column": "updated_at",
                "soft_delete_column": "",
            },
        ],
        "relations": [
            {
                "id": "rel_demo_author", "parent_table_id": "table_demo_books",
                "child_table_id": "table_demo_authors", "type": "many_to_one",
                "field_pairs": [{"parent_field": "author_id", "child_field": "id"}],
                "fixed_conditions": [],
            },
            {
                "id": "rel_demo_metrics", "parent_table_id": "table_demo_books",
                "child_table_id": "table_demo_metrics", "type": "one_to_one",
                "field_pairs": [{"parent_field": "id", "child_field": "book_id"}],
                "fixed_conditions": [],
            },
        ],
    }


def bootstrap(base_url):
    payload = api(base_url, "/api/bootstrap")
    if any(job["id"] == DEMO_JOB_ID for job in payload.get("jobs", [])):
        print("Demo job already exists; configuration and running state preserved.")
        return
    for name in ["catalog", "analytics"]:
        api(base_url, "/api/connections", {
            "id": f"conn_demo_{name}", "name": f"demo-{name}",
            "type": "postgresql", "host": "postgres", "port": 5432,
            "database": name,
        })
    job = demo_job()
    job["start"] = True
    api(base_url, "/api/jobs", job)
    print("Started book-catalog-demo → book_catalog_v1. Watch its baseline in Studio.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8180")
    args = parser.parse_args()
    bootstrap(args.url)

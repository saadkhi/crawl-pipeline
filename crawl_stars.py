# crawl_stars.py
import os
import time
import json
import math
import requests
import psycopg2
from datetime import datetime, timezone
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from db import get_connection

GITHUB_API = "https://api.github.com/graphql"
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")

REPO_FIELDS = """
  node {
    ... on Repository {
      id
      name
      owner { login }
      stargazerCount
      description
      url
      primaryLanguage { name }
      defaultBranchRef { name }
      updatedAt
    }
  }
"""

QUERY_TEMPLATE = """
query ($queryString: String!, $after: String) {
  rateLimit {
    limit
    cost
    remaining
    resetAt
  }
  search(query: $queryString, type: REPOSITORY, first: 100, after: $after) {
    repositoryCount
    pageInfo { endCursor hasNextPage }
    edges {
      %s
    }
  }
}
""" % REPO_FIELDS

HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v4+json"
}

def ensure_tables():
    conn = get_connection()
    with conn.cursor() as cur:
        with open("db_schema.sql", "r") as f:
            cur.execute(f.read())
    conn.commit()
    conn.close()

def ensure_progress_row(key="default"):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO crawl_progress (id, cursor, last_run)
            VALUES (%s, NULL, now())
            ON CONFLICT (id) DO NOTHING
        """, (key,))
    conn.commit()
    conn.close()

def read_progress(key="default"):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SELECT cursor FROM crawl_progress WHERE id=%s", (key,))
        row = cur.fetchone()
    conn.close()
    return row[0] if row else None

def write_progress(cursor_value, key="default"):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE crawl_progress
            SET cursor=%s, last_run=now()
            WHERE id=%s
        """, (cursor_value, key))
    conn.commit()
    conn.close()

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(min=1, max=30),
    retry=retry_if_exception_type(Exception)
)
def graphql_request(query, variables):
    resp = requests.post(GITHUB_API, json={"query": query, "variables": variables}, headers=HEADERS, timeout=60)
    data = resp.json()
    if resp.status_code != 200 or "errors" in data:
        raise Exception(str(data))
    return data

def upsert_repo(node):
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO repos (id, owner, name, full_name, url, description, language, default_branch, updated_at, first_seen_at, last_seen_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),now())
            ON CONFLICT (id) DO UPDATE SET
                owner = EXCLUDED.owner,
                name = EXCLUDED.name,
                full_name = EXCLUDED.full_name,
                url = EXCLUDED.url,
                description = EXCLUDED.description,
                language = EXCLUDED.language,
                default_branch = EXCLUDED.default_branch,
                updated_at = EXCLUDED.updated_at,
                last_seen_at = now()
        """, (
            node["id"],
            node["owner"]["login"],
            node["name"],
            f"{node['owner']['login']}/{node['name']}",
            node.get("url"),
            node.get("description"),
            node.get("primaryLanguage", {}).get("name") if node.get("primaryLanguage") else None,
            node.get("defaultBranchRef", {}).get("name") if node.get("defaultBranchRef") else None,
            node.get("updatedAt")
        ))

        cur.execute("""
            INSERT INTO repo_stars (repo_id, observed_at, stargazers)
            VALUES (%s, now(), %s)
            ON CONFLICT DO NOTHING
        """, (
            node["id"],
            node["stargazerCount"]
        ))

    conn.commit()
    conn.close()

def crawl_once(query_string, start_cursor=None, max_pages=10):
    cursor = start_cursor
    pages = 0

    while True:
        pages += 1
        if pages > max_pages:
            print("Reached max_pages")
            break

        
        variables = {"queryString": query_string, "after": cursor}
        data = graphql_request(QUERY_TEMPLATE, variables)

        edges = data["data"]["search"]["edges"]
        for edge in edges:
            if edge["node"]:
                upsert_repo(edge["node"])

        page_info = data["data"]["search"]["pageInfo"]
        cursor = page_info["endCursor"]
        write_progress(cursor)

        print(f"Fetched page {pages}, next? {page_info['hasNextPage']}")

        if not page_info["hasNextPage"]:
            break

def main():
    if not GITHUB_TOKEN:
        raise SystemExit("Missing GITHUB_TOKEN")

    ensure_tables()
    ensure_progress_row("stars")
    last_cursor = read_progress("stars")

    crawl_once("stars:>0", start_cursor=last_cursor, max_pages=5)

if __name__ == "__main__":
    main()

# crawl_stars.py
import os
import time
import json
import math
import requests
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

GITHUB_API = "https://api.github.com/graphql"
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")  # provided by Actions automatically
PG_HOST = os.environ.get("POSTGRES_HOST", "localhost")
PG_PORT = int(os.environ.get("POSTGRES_PORT", 5432))
PG_DB   = os.environ.get("POSTGRES_DB", "postgres")
PG_USER = os.environ.get("POSTGRES_USER", "postgres")
PG_PASS = os.environ.get("POSTGRES_PASSWORD", "")

# GraphQL fragment to fetch basic repo fields
REPO_FIELDS = """
  node {
    ... on Repository {
      id
      name
      owner { login }
      name
      stargazerCount
      description
      url
      primaryLanguage { name }
      defaultBranchRef { name }
      updatedAt
    }
  }
"""

# We'll use the search query to enumerate repositories. The search API is limited to 1000 results per search query,
# so the approach is to vary queries; for this example we'll sample by language and created year. For simplicity
# we use a broad query "stars:>0" and paginate through cursors.
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
    "Accept": "application/vnd.github.v4.idl"
}

def get_db_conn():
    return psycopg2.connect(
        dbname=PG_DB, user=PG_USER, password=PG_PASS, host=PG_HOST, port=PG_PORT
    )

def ensure_progress_row(conn, key="default"):
    with conn.cursor() as cur:
        cur.execute("INSERT INTO crawl_progress (id, cursor, last_run) VALUES (%s, %s, now()) ON CONFLICT (id) DO NOTHING", (key, None))
        conn.commit()

def read_progress(conn, key="default"):
    with conn.cursor() as cur:
        cur.execute("SELECT cursor FROM crawl_progress WHERE id=%s", (key,))
        r = cur.fetchone()
        return r[0] if r else None

def write_progress(conn, cursor_value, key="default"):
    with conn.cursor() as cur:
        cur.execute("UPDATE crawl_progress SET cursor=%s, last_run=now() WHERE id=%s", (cursor_value, key))
        conn.commit()

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=30), retry=retry_if_exception_type(Exception))
def graphql_request(query, variables):
    resp = requests.post(GITHUB_API, json={"query": query, "variables": variables}, headers=HEADERS, timeout=60)
    if resp.status_code >= 500:
        # retriable server error
        raise Exception(f"Server error {resp.status_code}")
    if resp.status_code == 401:
        raise Exception("Unauthorized: check GITHUB_TOKEN")
    data = resp.json()
    if 'errors' in data:
        # If errors are rate-limit related or something else, raise to handle
        # simply include message and optionally retry
        err_msgs = [e.get('message','') for e in data['errors']]
        raise Exception("GraphQL errors: " + "; ".join(err_msgs))
    return data

def ensure_tables(conn):
    with open("db_schema.sql","r") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()

def upsert_repo_and_star(conn, repo_node):
    # repo_node has: id, name, owner.login, stargazerCount, description, url, primaryLanguage, defaultBranchRef, updatedAt
    rid = repo_node.get("id")
    owner = repo_node.get("owner", {}).get("login")
    name = repo_node.get("name")
    full_name = f"{owner}/{name}"
    url = repo_node.get("url")
    desc = repo_node.get("description")
    lang = repo_node.get("primaryLanguage", {}).get("name") if repo_node.get("primaryLanguage") else None
    branch = repo_node.get("defaultBranchRef", {}).get("name") if repo_node.get("defaultBranchRef") else None
    updated_at = repo_node.get("updatedAt")
    stars = repo_node.get("stargazerCount", 0)

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
              last_seen_at = now();
        """, (rid, owner, name, full_name, url, desc, lang, branch, updated_at))
        cur.execute("INSERT INTO repo_stars (repo_id, observed_at, stargazers) VALUES (%s, now(), %s) ON CONFLICT DO NOTHING", (rid, stars))
    conn.commit()

def iso_to_dt(s):
    return datetime.fromisoformat(s.replace('Z','+00:00')) if s else None

def crawl_once(conn, query_string, start_cursor=None, max_pages=20):
    cursor = start_cursor
    pages = 0
    while True:
        pages += 1
        if pages > max_pages:
            print("Reached page limit for this run:", max_pages)
            break
        variables = {"queryString": query_string, "after": cursor}
        data = graphql_request(QUERY_TEMPLATE, variables)
        # check rate limit
        rl = data.get("data", {}).get("rateLimit", {})
        remaining = rl.get("remaining")
        reset_at = rl.get("resetAt")
        cost = rl.get("cost")
        print(f"GraphQL cost={cost}, remaining={remaining}, resetAt={reset_at}")
        if remaining is not None and remaining < 50:
            # throttle - sleep until reset (plus a small buffer)
            if reset_at:
                reset_time = iso_to_dt(reset_at)
                now = datetime.now(timezone.utc)
                sleep_secs = (reset_time - now).total_seconds() + 5
                if sleep_secs > 0:
                    print("Sleeping due to low remaining points for", sleep_secs, "secs")
                    time.sleep(sleep_secs)
        search = data.get("data", {}).get("search", {})
        edges = search.get("edges", [])
        for e in edges:
            node = e.get("node")
            if node:
                upsert_repo_and_star(conn, node)
        page_info = search.get("pageInfo", {})
        cursor = page_info.get("endCursor")
        has_next = page_info.get("hasNextPage")
        # store progress
        write_progress(conn, cursor)
        print(f"Processed page {pages}, has_next={has_next}, cursor={cursor}")
        if not has_next:
            break

def main():
    if not GITHUB_TOKEN:
        raise SystemExit("GITHUB_TOKEN not set in environment (Actions provides it automatically).")

    # sample query - adjust as needed; avoid too large result sets per search since Search caps out.
    # We'll fetch repos with at least 1 star. This is a sampler. For production you'd vary queries (language, created: ranges)
    query_string = "stars:>0"  # simple sample; you can make the query more specific to partition the index space
    max_pages_per_run = int(os.environ.get("MAX_PAGES_PER_RUN", "10"))

    conn = get_db_conn()
    ensure_tables(conn)
    ensure_progress_row(conn, key="stars_crawl")
    start_cursor = read_progress(conn, key="stars_crawl")

    try:
        crawl_once(conn, query_string, start_cursor=start_cursor, max_pages=max_pages_per_run)
    finally:
        conn.close()

if __name__ == "__main__":
    main()

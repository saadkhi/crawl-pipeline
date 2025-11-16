# crawl_stars.py
import os
import asyncio
import aiohttp
import asyncpg
from datetime import datetime, timezone
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from typing import List, Dict, Any, Optional

# Configuration
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
CONCURRENT_REQUESTS = 10  # Adjust based on rate limits
BATCH_SIZE = 100  # Number of repos to process in a batch

# GraphQL Query
REPO_FIELDS = """
    id
    name
    owner { login }
    stargazerCount
    description
    url
    primaryLanguage { name }
    defaultBranchRef { name }
    updatedAt
"""

GRAPHQL_QUERY = """
query ($query: String!, $after: String) {
  search(query: $query, type: REPOSITORY, first: 100, after: $after) {
    repositoryCount
    pageInfo { endCursor hasNextPage }
    nodes {
      ... on Repository {
        """ + REPO_FIELDS + """
      }
    }
  }
}
"""

class GitHubCrawler:
    def __init__(self):
        self.headers = {
            "Authorization": f"bearer {GITHUB_TOKEN}",
            "Content-Type": "application/json"
        }
        self.session = None
        self.db_pool = None

    async def init_db(self):
        self.db_pool = await asyncpg.create_pool(DATABASE_URL)
        async with self.db_pool.acquire() as conn:
            with open("db_schema.sql", "r") as f:
                await conn.execute(f.read())
            await conn.execute("""
                INSERT INTO crawl_progress (id, cursor, last_run)
                VALUES ('stars', NULL, now())
                ON CONFLICT (id) DO NOTHING
            """)

    async def get_last_cursor(self) -> Optional[str]:
        async with self.db_pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT cursor FROM crawl_progress WHERE id = 'stars'"
            )

    async def update_cursor(self, cursor: str):
        async with self.db_pool.acquire() as conn:
            await conn.execute(
                "UPDATE crawl_progress SET cursor = $1, last_run = now() WHERE id = 'stars'",
                cursor
            )

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60)
    )
    async def fetch_page(self, query: str, cursor: str = None) -> Dict:
        variables = {"query": query, "after": cursor}
        async with self.session.post(
            "https://api.github.com/graphql",
            json={"query": GRAPHQL_QUERY, "variables": variables},
            headers=self.headers
        ) as response:
            if response.status != 200:
                error_text = await response.text()
                raise Exception(f"GitHub API error: {error_text}")
            return await response.json()

    async def process_batch(self, repos: List[Dict]):
        if not repos:
            return

        async with self.db_pool.acquire() as conn:
            # Batch insert/update repos
            await conn.executemany("""
                INSERT INTO repos (
                    id, owner, name, full_name, url, description, 
                    language, default_branch, updated_at, first_seen_at, last_seen_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now(), now())
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
            """, [(
                repo["id"],
                repo["owner"]["login"],
                repo["name"],
                f"{repo['owner']['login']}/{repo['name']}",
                repo.get("url"),
                repo.get("description"),
                repo.get("primaryLanguage", {}).get("name") if repo.get("primaryLanguage") else None,
                repo.get("defaultBranchRef", {}).get("name") if repo.get("defaultBranchRef") else None,
                repo.get("updatedAt")
            ) for repo in repos])

            # Batch insert stars
            await conn.executemany("""
                INSERT INTO repo_stars (repo_id, observed_at, stargazers)
                VALUES ($1, now(), $2)
                ON CONFLICT DO NOTHING
            """, [(repo["id"], repo["stargazerCount"]) for repo in repos])

    async def crawl(self, query: str, max_pages: int = 20):
        cursor = await self.get_last_cursor()
        page_count = 0
        processed_repos = 0

        async with aiohttp.ClientSession() as self.session:
            while page_count < max_pages:
                try:
                    result = await self.fetch_page(query, cursor)
                    search = result.get("data", {}).get("search", {})
                    repos = search.get("nodes", [])
                    page_info = search.get("pageInfo", {})
                    
                    if not repos:
                        break

                    # Process repos in batches
                    for i in range(0, len(repos), BATCH_SIZE):
                        batch = repos[i:i + BATCH_SIZE]
                        await self.process_batch(batch)

                    processed_repos += len(repos)
                    cursor = page_info.get("endCursor")
                    await self.update_cursor(cursor)

                    print(f"Processed page {page_count + 1}: {len(repos)} repos (total: {processed_repos})")

                    if not page_info.get("hasNextPage", False):
                        break

                    page_count += 1

                except Exception as e:
                    print(f"Error processing page {page_count + 1}: {str(e)}")
                    raise

async def main():
    if not GITHUB_TOKEN:
        raise SystemExit("Error: GITHUB_TOKEN environment variable is not set")
    if not DATABASE_URL:
        raise SystemExit("Error: DATABASE_URL environment variable is not set")

    crawler = GitHubCrawler()
    await crawler.init_db()
    
    # More specific query to get trending repos updated in the last week
    query = "stars:>1000 pushed:>2024-01-01 sort:stars-desc"
    
    print("Starting crawl...")
    await crawler.crawl(query, max_pages=10)  # Adjust max_pages as needed
    print("Crawl completed successfully!")

if __name__ == "__main__":
    asyncio.run(main())
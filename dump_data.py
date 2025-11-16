# dump_data.py
import csv
from db import get_connection

conn = get_connection()
cur = conn.cursor()

cur.execute("""
    SELECT r.full_name, s.observed_at, s.stargazers
    FROM repo_stars s
    JOIN repos r ON r.id = s.repo_id
    ORDER BY s.observed_at DESC
""")

rows = cur.fetchall()

with open("repo_stars.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["full_name", "observed_at", "stargazers"])
    writer.writerows(rows)

cur.close()
conn.close()

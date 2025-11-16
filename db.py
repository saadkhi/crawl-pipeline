# db.py
import os
import psycopg2

def get_connection():
    db_url = os.environ["DATABASE_URL"]
    return psycopg2.connect(db_url)

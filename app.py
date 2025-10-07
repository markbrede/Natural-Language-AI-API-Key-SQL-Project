import os, re, json
import mysql.connector as mysql
from dotenv import load_dotenv
from tabulate import tabulate
from openai import OpenAI

load_dotenv()
client = OpenAI()
MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

DB_CFG = dict(
    host=os.getenv("MYSQL_HOST", "localhost"),
    port=int(os.getenv("MYSQL_PORT", "3306")),
    user=os.getenv("MYSQL_USER", "root"),
    password=os.getenv("MYSQL_PASSWORD", ""),
    database=os.getenv("MYSQL_DB", "campus_vending"),
)

def get_schema_ddl() -> str:
    """Fetch real CREATE TABLE DDL for all tables in the target schema."""
    cn = mysql.connect(**DB_CFG)
    try:
        cur = cn.cursor()
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = %s ORDER BY table_name",
            (DB_CFG["database"],),
        )
        tables = [r[0] for r in cur.fetchall()]
        ddl_parts = []
        for t in tables:
            cur.execute(f"SHOW CREATE TABLE `{t}`")
            _tname, create_stmt = cur.fetchone()
            ddl_parts.append(create_stmt + ";")
        return "\n\n".join(ddl_parts)
    finally:
        cn.close()

PROMPT = """You translate a user question into a single MySQL SELECT query.

Rules:
- Output ONLY SQL (no prose, no backticks, no comments).
- Absolutely no DML/DDL (no INSERT/UPDATE/DELETE/ALTER/CREATE/DROP).
- Use explicit JOINs, short table aliases.
- Prefer safe defaults: if no limit is requested, add LIMIT 100.
- Use column and table names exactly as defined.

Here is the database DDL:
{schema}

Question:
{question}
"""

def generate_sql(question: str):
    schema = get_schema_ddl()
    p = PROMPT.format(schema=schema, question=question)
    resp = client.responses.create(
        model=MODEL,
        input=[
            {"role": "system", "content": "You are a careful SQL generator for MySQL."},
            {"role": "user", "content": p},
        ],
    )
    sql = getattr(resp, "output_text", "").strip()

    # Guardrails: only allow SELECT and nothing dangerous
    if not re.match(r"(?is)^\s*select\b", sql):
        raise ValueError("Model returned non-SELECT SQL:\n" + sql)

    # Optional: if user didn't ask for a limit and none present, add one
    if not re.search(r"(?is)\blimit\s+\d+\b", sql):
        sql = sql.rstrip().rstrip(";") + " LIMIT 100;"

    return sql

def run_sql(sql: str):
    cn = mysql.connect(**DB_CFG)
    try:
        cur = cn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description] if cur.description else []
        return cols, rows
    finally:
        cn.close()

def summarize(question, cols, rows):
    preview = {"columns": cols, "rows": rows[:50]}
    resp = client.responses.create(
        model=MODEL,
        input=[
            {"role": "system", "content": "You write short, clear answers for non-technical users."},
            {"role": "user", "content": f"Question: {question}\nResult JSON: {json.dumps(preview, default=str)}\nWrite a concise answer in plain English."},
        ],
    )
    return getattr(resp, "output_text", "").strip()

def main():
    print("Ask about campus_vending (type 'exit' to quit).")
    while True:
        q = input("\nQ> ").strip()
        if q.lower() in {"exit", "quit"}:
            break
        try:
            sql = generate_sql(q)
            print("\n[SQL]\n", sql)
            cols, rows = run_sql(sql)
            if cols:
                print("\n[RESULT]")
                print(tabulate(rows, headers=cols, tablefmt="github"))
            else:
                print("\n[RESULT] (no rows)")
            print("\n[ANSWER]\n", summarize(q, cols, rows))
        except Exception as e:
            print("\n[ERROR]\n", e)

if __name__ == "__main__":
    main()

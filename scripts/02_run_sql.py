import argparse
import sqlite3
from pathlib import Path

import pandas as pd


def run_query(db_path: Path, sql_path: Path) -> pd.DataFrame:
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")

    if not sql_path.exists():
        raise FileNotFoundError(f"SQL not found: {sql_path}")

    query = sql_path.read_text(encoding="utf-8")

    con = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(query, con)
    finally:
        con.close()

    return df


def main():
    parser = argparse.ArgumentParser(description="Run a SQL query against SQLite and save results to CSV.")
    parser.add_argument("sql", type=str, help="Path to .sql file, e.g. sql/02_queries/04_churn_by_payment.sql")
    parser.add_argument("out", type=str, help="Path to output .csv, e.g. data/processed/04_churn_by_payment.csv")
    parser.add_argument(
        "--db",
        type=str,
        default="data/processed/telco.db",
        help="Path to SQLite DB (default: data/processed/telco.db)"
    )

    args = parser.parse_args()

    sql_path = Path(args.sql)
    out_path = Path(args.out)
    db_path = Path(args.db)

    out_path.parent.mkdir(parents=True, exist_ok=True)

    df = run_query(db_path=db_path, sql_path=sql_path)

    print("=== RESULT (head) ===")
    print(df.head(20).to_string(index=False))

    df.to_csv(out_path, index=False)
    print(f"\nSaved CSV: {out_path.resolve()}")


if __name__ == "__main__":
    main()

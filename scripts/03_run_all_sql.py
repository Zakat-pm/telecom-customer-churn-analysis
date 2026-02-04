from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import pandas as pd


DEFAULT_DB = Path("../data/processed/telco.db")
DEFAULT_SQL_DIR = Path("../sql/queries")
DEFAULT_OUT_DIR = Path("../data/processed")


def run_query(db_path: Path, sql_path: Path) -> pd.DataFrame:
    sql = sql_path.read_text(encoding="utf-8")
    con = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(sql, con)
    finally:
        con.close()
    return df


def list_sql_files(sql_dir: Path) -> list[Path]:

    return sorted(sql_dir.glob("*.sql"))


def build_out_path(out_dir: Path, sql_path: Path) -> Path:

    return out_dir / (sql_path.stem + ".csv")


def main():
    parser = argparse.ArgumentParser(description="Run all SQL queries and export results to CSV.")
    parser.add_argument("--db", type=str, default=str(DEFAULT_DB), help="Path to SQLite DB")
    parser.add_argument("--sql-dir", type=str, default=str(DEFAULT_SQL_DIR), help="Directory with .sql files")
    parser.add_argument("--out-dir", type=str, default=str(DEFAULT_OUT_DIR), help="Directory to save CSV outputs")
    args = parser.parse_args()

    db_path = Path(args.db)
    sql_dir = Path(args.sql_dir)
    out_dir = Path(args.out_dir)

    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path.resolve()}")

    if not sql_dir.exists():
        raise FileNotFoundError(f"SQL dir not found: {sql_dir.resolve()}")

    out_dir.mkdir(parents=True, exist_ok=True)

    sql_files = list_sql_files(sql_dir)
    if not sql_files:
        print(f"No .sql files found in: {sql_dir.resolve()}")
        return

    print(f"DB: {db_path.resolve()}")
    print(f"SQL dir: {sql_dir.resolve()}")
    print(f"Out dir: {out_dir.resolve()}")
    print("-" * 60)

    for sql_path in sql_files:
        out_path = build_out_path(out_dir, sql_path)
        print(f"Running: {sql_path.name}  ->  {out_path.name}")

        df = run_query(db_path=db_path, sql_path=sql_path)


        print(f"  rows: {len(df)} cols: {len(df.columns)}")
        df.to_csv(out_path, index=False)
        print("  saved.")

    print("-" * 60)
    print("Done.")


if __name__ == "__main__":
    main()

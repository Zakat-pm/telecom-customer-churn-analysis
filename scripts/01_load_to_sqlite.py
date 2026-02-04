import sqlite3
import pandas as pd
from pathlib import Path

CSV_PATH = Path("../data/raw/WA_Fn-UseC_-Telco-Customer-Churn.csv")
DB_PATH = Path("../data/processed/telco.db")

df = pd.read_csv(CSV_PATH)

df["TotalCharges"] = (
    df["TotalCharges"].astype(str).str.strip().replace("", "0").astype(float)
)
df["tenure"] = df["tenure"].astype(int)

DB_PATH.parent.mkdir(parents=True, exist_ok=True)
con = sqlite3.connect(DB_PATH)

df.to_sql("telco_customers", con, if_exists="replace", index=False)

cur = con.cursor()
cur.execute("CREATE INDEX IF NOT EXISTS idx_telco_churn ON telco_customers(Churn);")
cur.execute("CREATE INDEX IF NOT EXISTS idx_telco_contract ON telco_customers(Contract);")
cur.execute("CREATE INDEX IF NOT EXISTS idx_telco_internet ON telco_customers(InternetService);")
cur.execute("CREATE INDEX IF NOT EXISTS idx_telco_payment ON telco_customers(PaymentMethod);")
con.commit()

row_count = cur.execute("SELECT COUNT(*) FROM telco_customers;").fetchone()[0]
null_totalcharges = cur.execute(
    "SELECT COUNT(*) FROM telco_customers WHERE TotalCharges IS NULL;"
).fetchone()[0]

print("Rows:", row_count)
print("NULL TotalCharges:", null_totalcharges)

con.close()
print("DB created at:", DB_PATH)
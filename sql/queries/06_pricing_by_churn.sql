SELECT
  Churn,
  ROUND(AVG(MonthlyCharges), 2) AS avg_monthly_charges,
  ROUND(AVG(TotalCharges), 2) AS avg_total_charges,
  COUNT(*) AS customers
FROM telco_customers
GROUP BY Churn
ORDER BY Churn;

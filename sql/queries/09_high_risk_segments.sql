SELECT
  Contract,
  InternetService,
  PaymentMethod,
  CASE
    WHEN tenure BETWEEN 0 AND 12 THEN '0–12'
    WHEN tenure BETWEEN 13 AND 24 THEN '13–24'
    WHEN tenure BETWEEN 25 AND 48 THEN '25–48'
    WHEN tenure BETWEEN 49 AND 72 THEN '49–72'
    ELSE 'unknown'
  END AS tenure_band,
  COUNT(*) AS customers,
  SUM(CASE WHEN Churn = 'Yes' THEN 1 ELSE 0 END) AS churned,
  ROUND(
    1.0 * SUM(CASE WHEN Churn = 'Yes' THEN 1 ELSE 0 END) / COUNT(*),
    4
  ) AS churn_rate
FROM telco_customers
GROUP BY
  Contract,
  InternetService,
  PaymentMethod,
  tenure_band
HAVING customers >= 50
ORDER BY churn_rate DESC;
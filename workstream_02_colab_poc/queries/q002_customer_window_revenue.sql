WITH daily_customer AS (
  SELECT
    f.customer_id,
    c.segment,
    c.region,
    d.sale_date,
    COUNT(*) AS sale_line_count,
    ROUND(SUM(f.net_amount), 2) AS daily_revenue
  FROM ws2_poc.fact_sales f
  JOIN ws2_poc.dim_customer c
    ON f.customer_id = c.customer_id
  JOIN ws2_poc.dim_date d
    ON f.date_id = d.date_id
  WHERE d.sale_date >= DATE '2024-07-01'
    AND f.returned_flag = false
  GROUP BY f.customer_id, c.segment, c.region, d.sale_date
),
ranked_customer_days AS (
  SELECT
    customer_id,
    segment,
    region,
    sale_date,
    sale_line_count,
    daily_revenue,
    ROUND(
      SUM(daily_revenue) OVER (
        PARTITION BY customer_id
        ORDER BY sale_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
      ),
      2
    ) AS rolling_7_sale_day_revenue,
    ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER BY daily_revenue DESC, sale_date DESC
    ) AS revenue_day_rank
  FROM daily_customer
)
SELECT
  customer_id,
  segment,
  region,
  sale_date,
  sale_line_count,
  daily_revenue,
  rolling_7_sale_day_revenue,
  revenue_day_rank
FROM ranked_customer_days
WHERE revenue_day_rank <= 3
ORDER BY customer_id, revenue_day_rank
LIMIT 500


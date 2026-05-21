SELECT
  f.customer_id,
  c.segment,
  c.region,
  COUNT(*) AS sale_line_count,
  COUNT(DISTINCT f.product_id) AS distinct_products,
  ROUND(SUM(f.net_amount), 2) AS customer_revenue,
  ROUND(AVG(f.net_amount), 2) AS avg_sale_amount
FROM ws2_poc.fact_sales f
JOIN ws2_poc.dim_customer c
  ON f.customer_id = c.customer_id
WHERE f.returned_flag = false
GROUP BY f.customer_id, c.segment, c.region
HAVING SUM(f.net_amount) > 250
ORDER BY customer_revenue DESC
LIMIT 1000


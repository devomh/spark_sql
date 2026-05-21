SELECT
  d.year_month,
  p.category,
  s.region,
  COUNT(*) AS sale_line_count,
  COUNT(DISTINCT f.customer_id) AS distinct_customers,
  SUM(f.quantity) AS units_sold,
  ROUND(SUM(f.net_amount), 2) AS net_revenue,
  ROUND(AVG(f.net_amount), 2) AS avg_sale_amount
FROM ws2_poc.fact_sales f
JOIN ws2_poc.dim_date d
  ON f.date_id = d.date_id
JOIN ws2_poc.dim_product p
  ON f.product_id = p.product_id
JOIN ws2_poc.dim_store s
  ON f.store_id = s.store_id
WHERE d.year_month BETWEEN '2024-04' AND '2025-03'
  AND f.returned_flag = false
GROUP BY d.year_month, p.category, s.region
ORDER BY d.year_month, net_revenue DESC


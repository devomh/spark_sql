SELECT
  d.year_month,
  s.region,
  p.category,
  GROUPING_ID() AS grouping_level,
  COUNT(*) AS sale_line_count,
  ROUND(SUM(f.net_amount), 2) AS net_revenue,
  ROUND(SUM(f.quantity * p.standard_cost), 2) AS estimated_cost,
  ROUND(SUM(f.net_amount - (f.quantity * p.standard_cost)), 2) AS estimated_margin,
  ROUND(
    SUM(f.net_amount - (f.quantity * p.standard_cost)) / NULLIF(SUM(f.net_amount), 0),
    4
  ) AS estimated_margin_rate
FROM ws2_poc.fact_sales f
JOIN ws2_poc.dim_date d
  ON f.date_id = d.date_id
JOIN ws2_poc.dim_store s
  ON f.store_id = s.store_id
JOIN ws2_poc.dim_product p
  ON f.product_id = p.product_id
WHERE d.fiscal_year = 2025
  AND f.returned_flag = false
GROUP BY GROUPING SETS (
  (d.year_month, s.region, p.category),
  (d.year_month, p.category),
  (s.region, p.category),
  ()
)
ORDER BY grouping_level, net_revenue DESC

SELECT
  d.quarter_name,
  p.department,
  p.category,
  s.store_type,
  pr.promotion_type,
  COUNT(*) AS sale_line_count,
  ROUND(SUM(f.net_amount), 2) AS net_revenue,
  ROUND(AVG(f.discount_amount), 2) AS avg_discount
FROM ws2_poc.fact_sales f
JOIN ws2_poc.dim_date d
  ON f.date_id = d.date_id
JOIN ws2_poc.dim_product p
  ON f.product_id = p.product_id
JOIN ws2_poc.dim_store s
  ON f.store_id = s.store_id
LEFT JOIN ws2_poc.dim_promotion pr
  ON f.promotion_id = pr.promotion_id
WHERE d.fiscal_year IN (2024, 2025)
  AND p.is_premium = true
GROUP BY d.quarter_name, p.department, p.category, s.store_type, pr.promotion_type
ORDER BY net_revenue DESC
LIMIT 200

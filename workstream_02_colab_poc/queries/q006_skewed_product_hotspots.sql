WITH product_sales AS (
  SELECT
    f.product_id,
    p.category,
    p.department,
    s.region,
    COUNT(*) AS sale_line_count,
    ROUND(SUM(f.net_amount), 2) AS net_revenue,
    ROUND(AVG(f.net_amount), 2) AS avg_sale_amount
  FROM ws2_poc.fact_sales f
  JOIN ws2_poc.dim_product p
    ON f.product_id = p.product_id
  JOIN ws2_poc.dim_store s
    ON f.store_id = s.store_id
  WHERE f.returned_flag = false
  GROUP BY f.product_id, p.category, p.department, s.region
)
SELECT
  product_id,
  category,
  department,
  region,
  sale_line_count,
  net_revenue,
  avg_sale_amount,
  RANK() OVER (
    PARTITION BY region
    ORDER BY sale_line_count DESC, net_revenue DESC
  ) AS regional_volume_rank
FROM product_sales
ORDER BY regional_volume_rank, sale_line_count DESC
LIMIT 500


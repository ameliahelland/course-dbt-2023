WEEK 3
==

**Question 1:** *What is our overall conversion rate?*

**Answer:** *0.62*

**SQL:**

```
WITH purchase_sessions AS (
  SELECT
    DISTINCT e.session_id
  FROM dev_db.dbt_ameliasigmacomputingcom.stg_postgres__events e
  WHERE e.event_type = 'checkout'
),
total_sessions AS (
  SELECT
    DISTINCT e.session_id
  FROM dev_db.dbt_ameliasigmacomputingcom.stg_postgres__events e
)
SELECT
  ROUND(COUNT(DISTINCT purchase_sessions.session_id)::FLOAT / COUNT(DISTINCT total_sessions.session_id), 2) AS overall_conversion_rate
FROM purchase_sessions, total_sessions
```

________________


**Question 2:** *What is the conversion rate of each product?*

**Answer:**

|     Product Name     | Conversion Rate |
| :---                 |             ---:|
| Pothos               |             0.66|
| Snake Plant          |             0.66|
| Ponytail Palm        |             0.65|
| Angel Wings Begonia  |             0.64|
| Pink Anthurium       |             0.64|
| Spider Plant         |             0.64|
| Ficus                |             0.63|
| Bird of Paradise     |             0.63|
| Dragon Tree          |             0.63|
| Monstera             |             0.63|
| Peace Lily           |             0.63|
| Birds Nest Fern      |             0.63|
| Boston Fern          |             0.63|
| Money Tree           |             0.63|
| Fiddle Leaf Fig      |             0.62|
| Alocasia Polly       |             0.62|
| Calathea Makoyana    |             0.62|
| Orchid               |             0.62|
| Majesty Palm         |             0.61|
| Cactus               |             0.61|
| Philodendron         |             0.61|
| Devil's Ivy          |             0.61|
| Pilea Peperomioides  |             0.61|
| ZZ Plant             |             0.60|
| Bamboo               |             0.60|
| Jade Plant           |             0.59|
| Rubber Plant         |             0.59|
| String of pearls     |             0.59|
| Arrow Head           |             0.58|
| Aloe Vera            |             0.57|

**SQL:**

```
WITH valid_sessions AS (
  SELECT DISTINCT
    e.session_id,
    e.order_id
  FROM dev_db.dbt_ameliasigmacomputingcom.stg_postgres__events e
  WHERE e.event_type IN ('add_to_cart', 'checkout')
    AND e.order_id IS NOT NULL
),
product_sessions AS (
  SELECT
    p.product_id,
    p.name AS product_name,
    e.session_id
  FROM dev_db.dbt_ameliasigmacomputingcom.stg_postgres__products p
  JOIN dev_db.dbt_ameliasigmacomputingcom.stg_postgres__events e ON p.product_id = e.product_id
  WHERE e.session_id IN (SELECT session_id FROM valid_sessions)
),
session_product_counts AS (
  SELECT
    ps.session_id,
    ps.product_id,
    COUNT(DISTINCT ps.product_id) AS product_count
  FROM product_sessions ps
  GROUP BY ps.session_id, ps.product_id
),
session_order_counts AS (
  SELECT
    vs.session_id,
    COUNT(DISTINCT vs.order_id) AS order_count
  FROM valid_sessions vs
  GROUP BY vs.session_id
)
SELECT
  ps.product_name,
  SUM(CASE WHEN spc.product_count > 0 THEN 1 ELSE 0 END) AS total_sessions_added_to_cart,
  COUNT(DISTINCT CASE WHEN soc.order_count > 0 THEN spc.session_id END) AS total_sessions_purchased,
  CASE
    WHEN SUM(CASE WHEN spc.product_count > 0 THEN 1 ELSE 0 END) = 0 THEN 0.0
    ELSE COUNT(DISTINCT CASE WHEN soc.order_count > 0 THEN spc.session_id END)::FLOAT /
         SUM(CASE WHEN spc.product_count > 0 THEN 1 ELSE 0 END)
  END AS conversion_rate
FROM product_sessions ps
JOIN session_product_counts spc ON ps.session_id = spc.session_id AND ps.product_id = spc.product_id
JOIN session_order_counts soc ON ps.session_id = soc.session_id
GROUP BY ps.product_name
ORDER BY conversion_rate DESC
```

------------
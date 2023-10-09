WEEK 4
==

**Question 1:** *Which products had their inventory change from week 3 to week 4?*

**Answer:** *Pothos, Philodendron, Bamboo, ZZ Plant, Monstera, String of pearls*

**SQL:**

```
WITH InventoryChanges AS (
   SELECT
       product_id,
       name AS product_name,
       inventory,
       dbt_updated_at AS change_date,
       LAG(inventory) OVER (PARTITION BY product_id ORDER BY dbt_updated_at) AS previous_inventory
   FROM
       dev_db.dbt_ameliasigmacomputingcom.products_snapshot
)
SELECT
   product_id,
   product_name,
   change_date,
   inventory,
   previous_inventory,
   (inventory - previous_inventory) AS inventory_change
FROM
   InventoryChanges
WHERE
   previous_inventory IS NOT NULL
   AND inventory <> previous_inventory
   AND DATE_TRUNC('week', change_date) = DATE_TRUNC('week', CURRENT_DATE())
ORDER BY
   product_id, change_date
```

--------------

**Question 2:** *Which products had the most fluctuations in inventory?*

**Answer:** *The products with the most fluctuations in inventory are: String of pearls, ZZ Plant, Monstera, Bamboo, and Pothos*

**SQL:**

```
WITH InventoryChanges AS (
   SELECT
       product_id,
       name AS product_name,
       inventory,
       dbt_updated_at AS change_date,
       LAG(inventory) OVER (PARTITION BY product_id ORDER BY dbt_updated_at) AS previous_inventory
   FROM
       dev_db.dbt_ameliasigmacomputingcom.products_snapshot
),
VarianceData AS (
   SELECT
       product_id,
       COALESCE(VARIANCE(inventory), 0) AS variance
   FROM
       InventoryChanges
   WHERE
       change_date >= (CURRENT_DATE() - INTERVAL '3 weeks')
   GROUP BY
       product_id
)
SELECT
   ic.product_id,
   ic.product_name,
   MIN(ic.change_date) AS start_date,
   MAX(ic.change_date) AS end_date,
   MIN(ic.inventory) AS min_inventory,
   MAX(ic.inventory) AS max_inventory,
   ROUND(AVG(vd.variance), 2) AS average_variance
FROM
   InventoryChanges ic
JOIN
   VarianceData vd
ON
   ic.product_id = vd.product_id
GROUP BY
   ic.product_id, ic.product_name
ORDER BY
   average_variance DESC
LIMIT 5
```

--------------

**Question 3:** *Did we have any items go out of stock in the last 3 weeks?*

**Answer:** *Yes, String of pearls and Pothos ran out of inventory.*

**SQL:**

```
WITH InventoryChanges AS (
   SELECT
       product_id,
       name AS product_name,
       inventory,
       dbt_updated_at AS change_date
   FROM
       dev_db.dbt_ameliasigmacomputingcom.products_snapshot
)
SELECT
   product_id,
   product_name,
   MIN(change_date) AS start_date,
   MAX(change_date) AS end_date,
   COUNT(*) AS out_of_stock_count
FROM
   InventoryChanges
WHERE
   inventory = 0
   AND change_date >= (CURRENT_DATE() - INTERVAL '3 weeks')
GROUP BY
   product_id, product_name
```

--------------

**Question 4:** *How are our users moving through the product funnel?*

**Answer:** **

**SQL:**

```
WITH FunnelData AS (
    -- Calculate total site visits
    SELECT COUNT(DISTINCT event_id) AS total_site_visits
    FROM dev_db.dbt_ameliasigmacomputingcom.stg_postgres__events
),

ProductPageviews AS (
    -- Calculate total product pageviews
    SELECT 
        product_id, 
        COUNT(*) AS total_pageviews
    FROM dev_db.dbt_ameliasigmacomputingcom.stg_postgres__events
    WHERE event_type = 'page_view'
    GROUP BY product_id
),

ItemsAddedToCart AS (
    -- Calculate total items added to cart
    SELECT 
        product_id, 
        COUNT(DISTINCT cart_event_id) AS total_items_added_to_cart
    FROM dev_db.dbt_ameliasigmacomputingcom.dim_orders
    WHERE cart_event_id IS NOT NULL
    GROUP BY product_id
),

Checkouts AS (
    -- Calculate total checkouts
    SELECT 
        product_id, 
        COUNT(DISTINCT checkout_event_id) AS total_checkouts
    FROM dev_db.dbt_ameliasigmacomputingcom.dim_orders
    WHERE checkout_event_id IS NOT NULL
    GROUP BY product_id
), 

ProductFunnels AS (
    -- Join all the calculated metrics with product information
    SELECT
        dp.product_name,
        COALESCE(pp.total_pageviews, 0) AS total_product_pageviews,
        COALESCE(iac.total_items_added_to_cart, 0) AS total_items_added_to_cart,
        COALESCE(c.total_checkouts, 0) AS total_checkouts
    FROM dev_db.dbt_ameliasigmacomputingcom.dim_products dp
    LEFT JOIN ProductPageviews pp ON dp.product_id = pp.product_id
    LEFT JOIN ItemsAddedToCart iac ON dp.product_id = iac.product_id
    LEFT JOIN Checkouts c ON dp.product_id = c.product_id
),

TotalSiteVisits AS (
    -- Calculate the first value of total_site_visits
    SELECT
        MAX(total_site_visits) AS first_total_site_visits
    FROM FunnelData
)

SELECT
    tsv.first_total_site_visits AS total_site_visits,
    SUM(pf.total_product_pageviews) AS total_product_pageviews,
    SUM(pf.total_items_added_to_cart) AS total_items_added_to_cart,
    SUM(pf.total_checkouts) AS total_checkouts
FROM
    TotalSiteVisits tsv,
    ProductFunnels pf
GROUP BY total_site_visits
```

--------------

**Question 5:** *Which steps in the funnel have largest drop off points?*

**Answer:** *The cart dropoff rate is the greatest. This means that there are way more product pageviews than total products added to cart.*

**SQL:**

```
WITH FunnelData AS (
    -- Calculate total site visits
    SELECT COUNT(DISTINCT event_id) AS total_site_visits
    FROM dev_db.dbt_ameliasigmacomputingcom.stg_postgres__events
),

ProductPageviews AS (
    -- Calculate total product pageviews
    SELECT 
        product_id, 
        COUNT(*) AS total_pageviews
    FROM dev_db.dbt_ameliasigmacomputingcom.stg_postgres__events
    WHERE event_type = 'page_view'
    GROUP BY product_id
),

ItemsAddedToCart AS (
    -- Calculate total items added to cart
    SELECT 
        product_id, 
        COUNT(DISTINCT cart_event_id) AS total_items_added_to_cart
    FROM dev_db.dbt_ameliasigmacomputingcom.dim_orders
    WHERE cart_event_id IS NOT NULL
    GROUP BY product_id
),

Checkouts AS (
    -- Calculate total checkouts
    SELECT 
        product_id, 
        COUNT(DISTINCT checkout_event_id) AS total_checkouts
    FROM dev_db.dbt_ameliasigmacomputingcom.dim_orders
    WHERE checkout_event_id IS NOT NULL
    GROUP BY product_id
), 

ProductFunnels AS (
    -- Join all the calculated metrics with product information
    SELECT
        dp.product_name,
        COALESCE(pp.total_pageviews, 0) AS total_product_pageviews,
        COALESCE(iac.total_items_added_to_cart, 0) AS total_items_added_to_cart,
        COALESCE(c.total_checkouts, 0) AS total_checkouts
    FROM dev_db.dbt_ameliasigmacomputingcom.dim_products dp
    LEFT JOIN ProductPageviews pp ON dp.product_id = pp.product_id
    LEFT JOIN ItemsAddedToCart iac ON dp.product_id = iac.product_id
    LEFT JOIN Checkouts c ON dp.product_id = c.product_id
),

TotalSiteVisits AS (
    -- Calculate the first value of total_site_visits
    SELECT
        MAX(total_site_visits) AS first_total_site_visits
    FROM FunnelData
)

SELECT
    tsv.first_total_site_visits AS total_site_visits,
    SUM(pf.total_product_pageviews) AS total_product_pageviews,
    SUM(pf.total_items_added_to_cart) AS total_items_added_to_cart,
    SUM(pf.total_checkouts) AS total_checkouts,
    1.0 - SUM(pf.total_product_pageviews) / tsv.first_total_site_visits AS product_pageview_dropoff_rate,
    1.0 - SUM(pf.total_items_added_to_cart) / SUM(pf.total_product_pageviews) AS cart_dropoff_rate,
    1.0 - SUM(pf.total_checkouts) / SUM(pf.total_items_added_to_cart) AS checkout_dropoff_rate
FROM
    TotalSiteVisits tsv,
    ProductFunnels pf
GROUP BY total_site_visits
```

--------------

**Question 6:** *If your organization is using dbt, what are 1-2 things you might do differently / recommend to your organization based on learning from this course?*

**Answer:** *I'd make some dimension and fact tables so folks could make their own models more easily, in case something comes up that wasn't accounted for in the mega-table's construction logic. I'd also go to each department and ask what data they need to understand their operational performance more easily.*

--------------

**Question 7:** *If you are thinking about moving to analytics engineering, what skills have you picked that give you the most confidence in pursuing this next step?*

**Answer:** *I'm excited to have picked up better SQL and modeling skills (it really helps to construct the project in a modular way). I'm also excited to have learned how to make dbt pipelines! These skills are critical for success as an analytics engineer, and I feel proficient enough to learn more on my own.*
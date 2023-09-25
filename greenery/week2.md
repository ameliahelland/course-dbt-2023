WEEK 2
==

**Question 1:** *What is our user repeat rate?*
**Answer:** *80%*
**SQL:**

```
WITH orders_placed_per_user AS (
    SELECT
        user_id AS UserId,
        count(distinct order_id) AS DistinctOrdersPlaced
    FROM dev_db.dbt_ameliasigmacomputingcom.stg_postgres__orders
    GROUP BY user_id
)
SELECT
    ROUND((COUNT_IF(DistinctOrdersPlaced > 1) / COUNT(*)), 2) AS RepeatUserRate
FROM
    orders_placed_per_user
```


**Question 2.1:** *What are good indicators of a user who will likely purchase again?*
**Answer:** *Users with more than one session are more likely to be repeat customers.*
**SQL:**

```
WITH DateSpine AS (
    SELECT
        DATEADD(
            HOUR, 
            seq4(), 
            DATE_TRUNC(
                'HOUR', 
                (
                    SELECT 
                        MIN(e.created_at) 
                    FROM dev_db.dbt_ameliasigmacomputingcom.stg_postgres__events e
                )
            )
        ) AS Hour
    FROM
        TABLE(GENERATOR(ROWCOUNT => 48)) -- Adjust the ROWCOUNT as needed
),
UserActivity AS (
    SELECT
        u.user_id AS UserId,
        ds.Hour AS Hour,
        COUNT(DISTINCT e.order_id) AS distinct_order_count,
        COALESCE(COUNT(DISTINCT e.session_id), 0) AS distinct_session_count,
        COALESCE(COUNT(CASE WHEN e.event_type = 'page_view' THEN e.session_id END), 0) AS distinct_pageview_count,
        COALESCE(COUNT(CASE WHEN e.event_type = 'add_to_cart' THEN e.session_id END), 0) AS distinct_add_to_cart_count
    FROM
        dev_db.dbt_ameliasigmacomputingcom.stg_postgres__users u
    CROSS JOIN
        DateSpine ds
    LEFT JOIN
        dev_db.dbt_ameliasigmacomputingcom.stg_postgres__events e
    ON
        u.user_id = e.user_id
    AND
        ds.Hour <= e.created_at
    AND
        e.created_at < ds.Hour + interval '1 hour'
    GROUP BY
        u.user_id, ds.Hour
),
user_order_and_session_statistics AS (
    SELECT
        ua.UserId,
        ua.Hour,
        COALESCE(ua.distinct_order_count, 0) AS distinct_order_count,
        COALESCE(ua.distinct_session_count, 0) AS distinct_session_count,
        COALESCE(ua.distinct_pageview_count, 0) AS distinct_pageview_count,
        COALESCE(ua.distinct_add_to_cart_count, 0) AS distinct_add_to_cart_count
    FROM
        UserActivity ua
    ORDER BY
        ua.UserId, ua.Hour
)
SELECT
    EXTRACT(HOUR FROM uoss.Hour) AS OrderHour,
    SUM(distinct_order_count) AS TotalDistinctOrders,
    SUM(distinct_session_count) AS TotalDistinctSessions,
    SUM(distinct_pageview_count) AS TotalPageviews,
    SUM(distinct_add_to_cart_count) AS TotalItemsAddedToCart,
    ROUND(CORR(distinct_order_count, distinct_session_count) * 100, 2) AS CorrOrdersAndSessions,
    ROUND(CORR(distinct_order_count, distinct_pageview_count) * 100, 2) AS CorrOrdersAndPageviews,
    ROUND(CORR(distinct_order_count, distinct_add_to_cart_count) * 100, 2) AS CorrOrdersAndItemsInCart
FROM
    user_order_and_session_statistics uoss
GROUP BY
    OrderHour
ORDER BY
    OrderHour
```


**Question 2.2:** *What are good indicators that a user won't be a repeat customer?*
**Answer:** *Users with one or less sessions are less likely to be repeat customers.*
**SQL:**

```
WITH UserOrderStats AS (
    SELECT
        u.user_id,
        COUNT(DISTINCT o.order_id) AS distinct_order_count,
        COALESCE(SUM(oi.quantity * p.price), 0) AS total_order_amount,
        MAX(o.delivered_at) AS last_delivery_date
    FROM
        dev_db.dbt_ameliasigmacomputingcom.stg_postgres__users u
    LEFT JOIN
        dev_db.dbt_ameliasigmacomputingcom.stg_postgres__orders o ON u.user_id = o.user_id
    LEFT JOIN
        dev_db.dbt_ameliasigmacomputingcom.stg_postgres__order_items oi ON o.order_id = oi.order_id
    LEFT JOIN
        dev_db.dbt_ameliasigmacomputingcom.stg_postgres__products p ON oi.product_id = p.product_id
    GROUP BY
        u.user_id
),
UserSessionCounts AS (
    SELECT
        user_id,
        COUNT(DISTINCT session_id) AS session_count
    FROM
        dev_db.dbt_ameliasigmacomputingcom.stg_postgres__events
    GROUP BY
        user_id
)
SELECT
    u.user_id,
    u.first_name,
    u.last_name,
    u.created_at AS user_created_at,
    a.zipcode,
    a.state,
    a.country,
    COALESCE(uos.distinct_order_count, 0) AS distinct_order_count,
    COALESCE(uos.total_order_amount, 0) AS total_order_amount,
    COALESCE(usc.session_count, 0) AS session_count,
    uos.last_delivery_date
FROM
    dev_db.dbt_ameliasigmacomputingcom.stg_postgres__users u
LEFT JOIN
    dev_db.dbt_ameliasigmacomputingcom.stg_postgres__addresses a ON u.address_id = a.address_id
LEFT JOIN
    UserOrderStats uos ON u.user_id = uos.user_id
LEFT JOIN
    UserSessionCounts usc ON u.user_id = usc.user_id
ORDER BY
    COALESCE(uos.distinct_order_count, 0) ASC, uos.last_delivery_date ASC
```


**Question 2.3:** *If you had more data, what features would you want to look into to answer this question?*
**Answer:** *I'd want to see age and gender data to start with.*


**Question 3:** *Explain the product mart models you added. Why did you organize the models in the way you did?*
**Answer:** *I made the following models for the following reasons:*
    product_inventory_and_order_statistics:
        This model combines information related to product inventory and order statistics. It includes columns for the product name, the amount of
        product ordered (InventoryUsed), the current available inventory (Inventory), and the average amount of the product purchased per order 
        (AverageAmountPurchasedPerOrder). By including both inventory and order statistics in a single model, it allows for analysis of how product 
        availability and ordering behavior are related. For example, you can analyze whether low inventory levels impact the average amount purchased 
        per order.
   product_pageviews:
        This model focuses on tracking product pageviews. It includes columns for the product name and the number of pageviews per product 
        (PageViews). Keeping pageview data separate from other product-related data allows for analysis of how user interactions and product views 
        correlate with sales or inventory management.
   product_revenues:
        This model is dedicated to tracking product revenues. It includes columns for the product name and the revenue generated per product over a 
        specified orders timeframe (Revenue).
        Separating revenue data into its own model facilitates revenue analysis, pricing strategy evaluation, and identification of top-performing 
        products in terms of revenue generation.
   related_products:
         This model is designed to find and store information about related products. It includes columns for the base product, frequently ordered 
         with product, and the frequency of these two products being ordered together. The purpose of this model is likely to support product 
         recommendations and cross-selling strategies. By identifying frequently co-purchased products, it can help improve product recommendations 
         for users, potentially increasing sales.
 In summary, the organization of these models reflects a modular approach to managing and analyzing different aspects of product data. This 
 separation of concerns allows for more focused and specialized analysis of various product-related aspects, from inventory and order behavior to
 pageviews, revenues, and product relationships. It enables a deeper understanding of product performance and customer behavior, which can inform 
 decision-making and marketing strategies.


 **Question 4:** *Show the DAG for your project.*
 **Answer:**
 <img src="/downloads/dbt-dag.png" alt="DAG" style="height: 100px; width:100px;"/>


 **Question 5.1:** *What assumptions are you making about each model? (i.e. why are you adding each test?)*
 **Answer:** *I'm assuming that the counts of things like sessions, orders, and pageviews should be positive.*


 **Question 5.2:** *Did you find any “bad” data as you added and ran tests on your models? How did you go about either cleaning the data in the dbt model or adjusting your assumptions/tests?*
 **Answer:** *I didn't find any bad data with my tests so I didn't clean anything.*


 **Question 6:** *Explain how you would ensure these tests are passing regularly and how you would alert stakeholders about bad data getting through.*
 **Answer:** *I'd write a cron job to run the dbt tests on a schedule, and I'd alert stakeholders by scheduling an export of the bad data rows.*


 **Question 7:** *Which products had their inventory change from week 1 to week 2?*
 **Answer:** *Pothos, Philodendron, Monstera, String of pearls have had their inventory change between week 1 and week 2.*
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
ORDER BY
    product_id, change_date
```
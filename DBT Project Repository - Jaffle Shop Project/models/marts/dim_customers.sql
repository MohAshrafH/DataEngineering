-- =========================================================
-- dbt MODEL PURPOSE
-- ---------------------------------------------------------
-- This model builds a CUSTOMER-LEVEL SUMMARY table.
-- One row per customer, enriched with:
--   • first order date
--   • most recent order date
--   • total number of orders
--
-- Source data comes from raw.jaffle_shop.customers
-- and raw.jaffle_shop.orders
-- =========================================================


/*
============ Version 2 ==============
*/
-- =========================================================
-- dim_customers
-- One row per customer with order history and lifetime value
-- =========================================================

with customers as (

    select *
    from {{ ref('stg_jaffle_shop__customers') }}

),

orders as (

    select *
    from {{ ref('stg_jaffle_shop__orders') }}

),

customer_orders as (

    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders
    from orders
    group by 1

),

customer_lifetime_value as (

    -- Sum all order amounts per customer
    select
        customer_id,
        sum(amount) as lifetime_value
    from {{ ref('fct_orders') }}
    group by 1

),

final as (

    select
        c.customer_id,
        c.first_name,
        c.last_name,

        coalesce(co.first_order_date, null) as first_order_date,
        coalesce(co.most_recent_order_date, null) as most_recent_order_date,
        coalesce(co.number_of_orders, 0) as number_of_orders,

        -- Total amount the customer has spent across all orders
        coalesce(clv.lifetime_value, 0) as lifetime_value

    from customers c
    left join customer_orders co
        on c.customer_id = co.customer_id
    left join customer_lifetime_value clv
        on c.customer_id = clv.customer_id

)

select * from final


/*
============ Version 1 ==============

with customers as (

select * from {{ ref('stg_jaffle_shop__customers') }}

),


orders as (

select * from {{ ref('stg_jaffle_shop__orders') }}

),


customer_orders as (

    -- ---------------------------------------------
    -- AGGREGATE ORDERS PER CUSTOMER
    -- ---------------------------------------------
    -- For each customer:
    --   • first_order_date  → earliest order
    --   • most_recent_order_date → latest order
    --   • number_of_orders → total orders count
    -- ---------------------------------------------
    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from orders

    -- Group by customer to collapse many orders into one row
    group by 1

),


final as (

    -- ---------------------------------------------
    -- FINAL CUSTOMER VIEW
    -- ---------------------------------------------
    -- Left join customers with their aggregated orders
    -- Left join ensures customers with NO orders are kept
    -- ---------------------------------------------
    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,

        -- Replace NULL with 0 for customers who never ordered
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders

    from customers

    left join customer_orders using (customer_id)

)

-- ---------------------------------------------------------
-- FINAL SELECT
-- ---------------------------------------------------------
-- dbt models always end with a SELECT statement
-- This is the dataset materialized as a view or table
-- ---------------------------------------------------------
select * from final
*/
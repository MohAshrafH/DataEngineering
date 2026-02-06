-- fct_orders
-- One row per order with total paid amount sourced from Stripe payments

with orders as (

    select
        order_id,
        customer_id
    from {{ ref('stg_jaffle_shop__orders') }}

),

payments as (

    select
        order_id,
        amount
    from {{ ref('stg_stripe__payments') }}

),

order_payments as (

    select
        order_id,
        sum(amount) as amount
    from payments
    group by 1

),

final as (

    select
        o.order_id,
        o.customer_id,

        -- If an order has no matching payment rows, p.amount will be NULL.
        -- COALESCE converts that NULL to 0 so the fact has a numeric amount.
        coalesce(p.amount, 0) as amount

    from orders o
    left join order_payments p
        on o.order_id = p.order_id

)

select * from final

-- part 2a

-- question 1

/* 
 * To find the most ordered item based on the number of times it appears in an order cart that checked out successfully,
 *  we need to analyze the data in the line_items table and consider only the orders that were successfully checked out.
 * We can achieve this by joining the line_items table with the orders table,
 * filtering out the orders with a "success" status, and then aggregating the quantity of each product ordered.
 * */
SELECT
    li.item_id AS product_id,
    p.name AS product_name,
    SUM(li.quantity) AS num_times_in_successful_orders
FROM
    line_items li
JOIN
    orders o ON li.order_id = o.order_id
JOIN
    products p ON li.item_id = p.id
WHERE
    o.status = 'success'
GROUP BY
    li.item_id,
    p.name
ORDER BY
    num_times_in_successful_orders DESC
LIMIT 1;



-- part 2a
-- question 2
/*
 * how I solved is, to create a cte of succesful order, by using order table and events
 * table, which we join by customer id with customer tablein order to extract the item id
 * and quanitity, where the status of checkout is successful 
 * next is to know the total spent, by joing successful orders with products table
 * this gave the opportunity to do the calculation to find the amount spent. 
 * (problem I encountered was for successful orders, in the where clause, i tried
 * linking checkout status of event data to the order status, it was returning NULL
 * for all the data. I didn't even continue.   
 */
WITH successful_orders AS (
    SELECT 
        o.order_id, 
        o.customer_id,
        c.location,
        e.event_data ->> 'item_id' AS item_id,
        e.event_data ->> 'quantity' AS quantity
    FROM orders o
    JOIN events e ON o.customer_id = e.customer_id
    JOIN customers c ON o.customer_id = c.customer_id
    WHERE o.status = 'success'
),

TotalSpend  AS (
	SELECT customer_id,
			location,
			SUM(p.price * CAST(so.quantity AS numeric)) AS total_spend
			
	FROM successful_orders so
	JOIN products p ON CAST(so.item_id AS INTEGER) = p.id
	GROUP BY
        customer_id,
        location
)

SELECT
    customer_id,
    location,
    COALESCE(total_spend, 0) AS total_spend
FROM
    TotalSpend
ORDER BY
    total_spend DESC
LIMIT
    5;


/*
SELECT * FROM successful_orders;


WITH successful_orders AS (
    SELECT o.order_id, o.customer_id,
    e.event_data ->> 'item_id' AS item_id,
        e.event_data ->> 'quantity' AS quantity
    FROM orders o
    JOIN events e ON o.customer_id = e.customer_id
    WHERE o.status = 'success'
     
)
SELECT * FROM successful_orders;



WITH OrderEvents AS (
    SELECT
        o.customer_id,
        c.location,
        e.event_data ->> 'order_id' AS order_id,
        e.event_data ->> 'item_id' AS item_id,
        e.event_data ->> 'quantity' AS quantity
    FROM
        orders o
    JOIN
        events e ON o.order_id = (e.event_data ->> 'order_id')::uuid
    JOIN
        customers c ON o.customer_id = c.customer_id
    WHERE
        o.status = 'success' 
        AND e.event_data::jsonb ->> 'event_type' = 'checkout'
        AND e.event_data::jsonb ->> 'status' = 'success'
)
SELECT * FROM OrderEvents;
*/


   
   
-- part 2b
-- question 1 (return location and checkot count of successful orders)
SELECT 
	c.location,
	count( e.event_data ->> 'event_type' = 'checkout')AS checkout_count
FROM events e 
JOIN 
	customers c on c.customer_id = e.customer_id 
WHERE 
	e.event_data ->> 'status' = 'success'
group by 
 	c.location
ORDER BY
    checkout_count desc
limit 1
	



-- question 2 (return customer id, and number of events before abandoning cart excluding visit)
/*
 * the plan is to find abandoned carts, that means carts = dropped, and to count 
 * the number of events before they abandoned carts we are tracking customer patterns
 * we are looking at stautus = 'failed', reached checkout, 
 * we are looking at customers who removed cart also
 * we count the run (num ofvisits)
 */
with AbandonedCarts (
  customer_id,
  abandonment_time
) as (
  select
    customer_id,
    min(event_timestamp) as abandonment_time
  from events
  where
    event_data::jsonb->>'event_type' in ('checkout', 'remove_from_cart')  -- Check for various abandonment scenarios
    and event_data::jsonb->> 'status' = 'failed'
  group by customer_id
),

EventsBeforeAbandonment  as (
  select
    e.customer_id,
    count(*) as num_events
  from events e
  inner join AbandonedCarts ac on e.customer_id = ac.customer_id  
  where
    e.event_timestamp < ac.abandonment_time
    and e.event_data::jsonb->>'event_type' != 'visit'  -- Exclude visit events
  group by
    e.customer_id
)

select
  ac.customer_id,
  coalesce(eo.num_events, 0) as num_events  
from AbandonedCarts ac
left join EventsBeforeAbandonment eo on ac.customer_id = eo.customer_id;




-- question 3 (number of average visits by customer who completed checkout, cast to 2 d.ps)

/* find the completed orders, use cte completed orders to see checkouts which where succesful.
* number of visits will be times the visited before checkout is successful, 
* calculate event data type successful for distinct customers before timestamp of success.

*/
 WITH completed_checkouts AS (
    SELECT DISTINCT customer_id
    FROM events
    WHERE event_data::jsonb->>'event_type' = 'checkout'
    AND event_data::jsonb->>'status' = 'success'
),
customer_visits_before_checkout as (
    select 
        e.customer_id, 
        COUNT(*) as num_visits
    from 
        events e
    join 
        completed_checkouts c on e.customer_id = c.customer_id
    where 
        e.event_data::jsonb->>'event_type' = 'visit'
        and e.event_timestamp < (
            select MIN(event_timestamp)
            from events
            where event_data::jsonb->>'event_type' = 'checkout'
            and event_data::jsonb->>'status' = 'success'
            and customer_id = e.customer_id
        )
    group by
        e.customer_id
)
select distinct customer_id,
    AVG(num_visits)::numeric(10, 2) as average_visits
from 
    customer_visits_before_checkout
group by
	customer_id;

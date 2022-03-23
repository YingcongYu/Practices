--- Banklist Dataset - 1
SELECT state, count(*) as NumBanks FROM hw_table
GROUP BY state ORDER BY NumBanks DESC LIMIT 5;

--- Banklist Dataset - 2
select substr(closing_date, -2, 2) as yr, count(*) as NumBanks from hw_table
group by substr(closing_date, -2, 2) order by yr;

--- Chicago crime dataset - 1
CREATE TABLE IF NOT EXISTS crime_parquet_16_20(
    id bigint,
    case_number string,
    `date` bigint,
    block string,
    iucr string,
    primary_type string,
    description string,
    loc_desc string,
    arrest boolean,
    domestic boolean,
    beat string,
    district string,
    ward int,
    community_area string,
    fbi_code string,
    x_coordinate int,
    y_coordinate int,
    updated_on string,
    latitude float,
    longitude float,
    loc string
)
PARTITIONED BY(yr int)
stored as parquet;

--- Chicago crime dataset - 2
INSERT INTO ken_db.crime_parquet_16_20 partition (yr=2016)
SELECT * FROM chicago.crime_parquet WHERE yr=2016;
INSERT INTO ken_db.crime_parquet_16_20 partition (yr=2017)
SELECT * FROM chicago.crime_parquet WHERE yr=2017;
INSERT INTO ken_db.crime_parquet_16_20 partition (yr=2018)
SELECT * FROM chicago.crime_parquet WHERE yr=2018;
INSERT INTO ken_db.crime_parquet_16_20 partition (yr=2019)
SELECT * FROM chicago.crime_parquet WHERE yr=2019;
INSERT INTO ken_db.crime_parquet_16_20 partition (yr=2020)
SELECT * FROM chicago.crime_parquet WHERE yr=2020;

--- Chicago crime dataset - 2 - dynamic partition version
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;7

--- Chicago crime dataset - 3-a
SELECT yr, primary_type, count(*) as frequency FROM crime_parquet_16_20
WHERE yr = 2016 GROUP BY yr, primary_type ORDER BY frequency DESC LIMIT 10;

SELECT yr, primary_type, count(*) as frequency FROM crime_parquet_16_20
WHERE yr = 2017 GROUP BY yr, primary_type ORDER BY frequency DESC LIMIT 10;

SELECT yr, primary_type, count(*) as frequency FROM crime_parquet_16_20
WHERE yr = 2018 GROUP BY yr, primary_type ORDER BY frequency DESC LIMIT 10;

SELECT yr, primary_type, count(*) as frequency FROM crime_parquet_16_20
WHERE yr = 2019 GROUP BY yr, primary_type ORDER BY frequency DESC LIMIT 10;

SELECT yr, primary_type, count(*) as frequency FROM crime_parquet_16_20
WHERE yr = 2020 GROUP BY yr, primary_type ORDER BY frequency DESC LIMIT 10;

--- Chicago crime dataset - 3-a - window functions
SELECT yr, primary_type, frequency, ranking from
(SELECT yr, primary_type, count(*) as frequency,
rank() over (partition by yr order by count(*) DESC) as ranking
from crime_parquet_16_20 GROUP BY yr, primary_type) a
WHERE ranking <= 10;

--- Chicago crime dataset - 3-b
SELECT loc_desc, count(*) as frequency FROM crime_parquet_16_20
GROUP BY loc_desc ORDER BY frequency DESC LIMIT 10;

--- Chicago crime dataset - 3-c
SELECT loc_desc, primary_type, count(*) as frequency FROM crime_parquet_16_20
GROUP BY loc_desc, primary_type ORDER BY frequency DESC;

--- Retail_db dataset - 1
SELECT order_item_order_id, count(*) as item_count FROM order_items
GROUP BY order_item_order_id having item_count = 5;

--- Retail_db dataset - 1 - window function version
SELECT DISTINCT order_item_order_id, item_count FROM
(SELECT order_item_order_id, count(*) over (PARTITION BY order_item_order_id) as item_count FROM order_items) a
WHERE item_count = 5;

--- Retail_db dataset - 2
SELECT orders.order_customer_id, orders.order_id, count(*) as item_count FROM orders
INNER JOIN order_items
on orders.order_id = order_items.order_item_order_id
GROUP BY order_customer_id, order_id
having item_count = 5;

--- Retail_db dataset - 3
SELECT customers.customer_fname, customers.customer_id, order_info.order_id, order_info.item_count FROM
(SELECT orders.order_customer_id, orders.order_id, count(*) as item_count FROM orders
INNER JOIN order_items
on orders.order_id = order_items.order_item_order_id
GROUP BY orders.order_customer_id, orders.order_id
HAVING item_count = 5) as order_info
inner join customers
on customers.customer_id = order_info.order_customer_id;

--- Retail_db dataset - 3 - window function version
SELECT DISTINCT customers.customer_fname, customers.customer_id, a.order_item_order_id, a.item_count FROM
(SELECT order_items.order_item_order_id, count(*) over (PARTITION BY order_item_order_id) as item_count
FROM order_items) a
INNER JOIN orders
on orders.order_id = a.order_item_order_id
INNER JOIN customers
on customers.customer_id = orders.order_customer_id
WHERE item_count = 5;

--- Retail_db dataset - 4
SELECT category_name, sum(order_item_quantity) as sales FROM categories
INNER JOIN products on categories.category_id = products.product_category_id
INNER JOIN order_items on order_items.order_item_product_id = products.product_id
GROUP BY category_name ORDER BY sales DESC LIMIT 10;

--- Retail_db dataset - 5
SELECT product_name, sum(order_item_subtotal) as revenue FROM products
INNER JOIN order_items on products.product_id = order_items.order_item_product_id
INNER JOIN orders on orders.order_id = order_items.order_item_order_id
AND orders.order_status <> "CANCELED"
AND orders.order_status <> "SUSPECTED_FRAUD"
GROUP BY product_name ORDER BY revenue DESC LIMIT 10;

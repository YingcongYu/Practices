### Preparation for Banklist
from pyspark.sql.functions import *
df_banklist = spark.read.table('bank_db.banklist')

### Banklist Dataset - 1 - with new name
df_banklist.groupBy('state').agg(count('bankname').alias('NumBanks')).sort(desc('NumBanks')).show(5)

### Banklist Dataset - 1 - with no name
df_banklist.groupBy('state').count().sort(desc('count')).show(5)

### Banklist Dataset - 2
df_banklist = df_banklist.withColumn('yr', substring('closing_date', -2, 2))
df_banklist.groupBy('yr').count().sort('yr').show()

### Preparation for Chicago crime
from pyspark.sql.functions import *
crime_parquet_16_20 = spark.read.table('chicago.crime_parquet')

### Chicago crime dataset - 1
- done by preparation

### Chicago crime dataset - 2
crime_parquet_16_20.write.filter('yr <= 2020 AND yr >= 2016').partitionBy('yr').parquet('/data/crime_parquet_16_20')

### Chicago crime dataset - 3-a
from pyspark.sql.window import Window
df_3_a = crime_parquet_16_20.groupBy('yr', 'primary_type').count()
windowSpec = Window.partitionBy('yr').orderBy(desc('count'))
df_3_a.withColumn('ranking', rank().over(windowSpec)).filter('ranking <= 10').orderBy('yr', 'ranking').show()

### Chicago crime dataset - 3-b
crime_parquet_16_20.groupBy('loc_desc').count().sort(desc('count')).show(10)

### Chicago crime dataset - 3-c
crime_parquet_16_20.groupBy('loc_desc', 'primary_type').count().sort(desc('count')).show()

### Preparation for Retail_db
from pyspark.sql.functions import *
df_categories = spark.read.table('retail_db.categories')
df_customers = spark.read.table('retail_db.customers')
df_departments = spark.read.table('retail_db.departments')
df_orders = spark.read.table('retail_db.orders')
df_order_items = spark.read.table('retail_db.order_items')
df_products = spark.read.table('retail_db.products')

### Retail_db dataset - 1
df_1 = df_order_items.groupBy('order_item_order_id').count().filter(col('count') == 5)
df_1.show()

### Retail_db dataset - 2
df_2 = df_1.join(df_orders, df_orders.order_id == df_order_items.order_item_order_id)
df_2.select('order_customer_id', 'order_id', 'count').show()

### Retail_db dataset - 3
df_3 = df_2.join(df_customers, df_customers.customer_id == df_2.order_customer_id)
df_3.select('customer_fname', 'order_customer_id', 'order_id', 'count').show()

### Retail_db dataset - 4
df_4 = df_categories.join(df_products, df_products.product_category_id == df_categories.category_id) \
.join(df_order_items, df_order_items.order_item_product_id == df_products.product_id)
df_4.groupBy('category_name').sum('order_item_quantity').sort(desc('sum(order_item_quantity)')).show(10)

### Retail_db dataset - 5
df_5 = df_products.join(df_order_items, df_products.product_id == df_order_items.order_item_product_id)
df_5 = df_5.join(df_orders, ((df_orders.order_id == df_order_items.order_item_order_id) & (col('order_status') != 'CANCELED') & (col('order_status') != 'SUSPECTED_FRAUD')))
df_5.groupBy('product_name').agg(sum('order_item_subtotal').alias('revenue')).sort(desc('revenue')).show(10)

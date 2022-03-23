#Prep.
from pyspark.sql.functions import *
product = spark.read.table('retail_db.products')
product.printSchema()
items = spark.read.table('retail_db.order_items')
items.printSchema()

#1+2
top = items.groupBy('order_item_product_id').agg(sum('order_item_subtotal').alias('total_revenue')).sort(desc('total_revenue')).limit(10)
top10 = top.join(product, items.order_item_product_id == product.product_id).select('product_id','product_name', 'total_revenue')
top10.show()

#3
top10.write.option('header', 'true').option('sep', ':').csv('/user/ken/spark-handson/q5')

#4
test = spark.read.option('header', 'true').option('sep', ':').option('inferSchema', 'true').csv('/user/ken/spark-handson/q5')
test.printSchema()
test.show()

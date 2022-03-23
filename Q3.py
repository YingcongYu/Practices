#Prep
from pyspark.sql.functions import *
product = spark.read.table('retail_db.products')
product.printSchema()

#1
max_price = product.groupBy('product_category_id').max('product_price')
max_price.show()

#2 
order_price = max_price.sort(desc('max(product_price)'))
order_price.show()

#3+4
order_price.write.option('compression', 'gzip').option('sep', '#').csv('/user/ken/spark-handson/q30')

#3+4 - correct:
order_price1 = order_price.withColumn('product_category_id', order_price.product_category_id.cast('string')).withColumn('max(product_price)', order_price['max(product_price)'].cast('string'))
order_price2 = order_price1.string(concat_ws('###', order_price.product_category_id, order_price['max(product_price)']))
order_price2.write.option('compression', 'gzip').text('/user/ken/spark-handson/q30-correct')

#3+4 in SQL.
a = spark.sql("""
SELECT concat_ws("###", product_category_id, max(product_price)) AS output FROM retail_db.products
GROUP BY product_category_id ORDER BY max(product_price) DESC
""")
a.show()

#5 
order_price.repartition(1).write.option('compression', 'snappy').option('sep', '|').csv('/user/ken/spark-handson/q31')

#6
order_price.repartition(1).write.option('header', 'true').option('sep', '|').csv('/user/ken/spark-handson/q32')

#7
path2 = '/user/ken/spark-handson/q31'
path3 = '/user/ken/spark-handson/q32'
q31 = spark.read.csv(path2)
q32 = spark.read.option('header', 'true').csv(path3)
q31.show()
q32.show()


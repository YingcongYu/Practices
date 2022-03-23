#Prep
from pyspark.sql.functions import *
order = spark.read.table('retail_db.orders')
order.printSchema()

#1 + 2
close = order.filter("order_status == 'CLOSED' AND order_date LIKE '2013-08%'")
close.show()

#3
eachday = close.groupBy('order_date').count().sort('order_date')
eachday.show(31)

#4-1
eachday.write.parquet('/user/ken/spark-handson/q4')

#4-2
eachday.write.saveAsTable('ken_db.eachday')


#5
check = spark.read.parquet('/user/ken/spark-handson/q4')
check.show()

#SQL.
a = spark.sql("""
SELECT count(order_status), to_date(order_date) AS order_date FROM retail_db.orders
WHERE order_status == 'CLOSED' AND order_date LIKE '2013-08%'
GROUP BY order_date ORDER BY order_date
""")
a.show(31)

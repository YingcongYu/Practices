### Create tables and import data from text files
from pyspark.sql.functions import *
path_dept = '/data/spark/employee/dept.txt'
path_emp = '/data/spark/employee/emp.txt'
path_deptHeader = '/data/spark/employee/dept-with-header.txt'
path_empHeader = '/data/spark/employee/emp-with-header.txt'
df_dept = spark.read.option('inferSchema', True).option('delimiter', ',').csv(path_dept)
df_emp = spark.read.option('inferSchema', True).option('delimiter', ',').csv(path_emp)
df_deptHeader = spark.read.option('inferSchema', True).option('delimiter', ',').option('header', True).csv(path_deptHeader)
df_empHeader = spark.read.option('inferSchema', True).option('delimiter', ',').option('header', True).csv(path_empHeader)

### 1 - Spark DataFrame API - list total salary for each dept
df_empHeader.groupBy('DEPTNO').sum('SAL').show()

### 2 - Spark DataFrame API - list total number of employee and average salary for each dept
df_empHeader.groupBy('DEPTNO').agg(avg('SAL').alias('avg_sal'), count('NAME').alias('num_emp')).show()

### 3 - Spark DataFrame API - list the first hired employee's name for each dept
df_firstEmp = df_empHeader.groupBy('DEPTNO').agg(min('HIREDATE').alias('firstEmp')).withColumnRenamed('DEPTNO', 'DEPT_NO')
df_join = df_empHeader.join(df_firstEmp, (df_empHeader.HIREDATE == df_firstEmp.firstEmp))
df_join.select('NAME', 'DEPTNO', 'HIREDATE').show()

### 4 - Spark DataFrame API - list total employee salary for each city
df_empHeader.join(df_deptHeader, df_empHeader.DEPTNO == df_deptHeader.DEPT_NO).groupBy('LOC').sum('SAL').show()

### 5 - Spark DataFrame API - list employee's name and salary whose salary is higher than their manager
emp2 = df_empHeader.selectExpr('EMPNO AS EMP_NO', 'SAL AS EMP_SAL')
df_5 = df_empHeader.join(emp2, ((df_empHeader.MGR == emp2.EMP_NO) & (df_empHeader.SAL > emp2.EMP_SAL))).select('NAME', 'SAL').show()

### 6 - Spark DataFrame API - list employee's name and salary whose salary is higher than average salary of whole company
avg = df_empHeader.agg(avg('SAL')).first()
df_empHeader.select('NAME', 'SAL').filter(df_empHeader.SAL > avg[0]).show()

### 7 - Spark DataFrame API - list employee's name and dept name whose name start with "J"
df_7 = df_empHeader.join(df_deptHeader, df_empHeader.DEPTNO == df_deptHeader.DEPT_NO)
df_7.select('NAME', 'DEPT_NAME').filter(df_empHeader.NAME.startswith('J')).show()

### 8 - Spark DataFrame API - list 3 employee's name and salary with highest salary
df_empHeader.sort(desc('SAL')).select('NAME', 'SAL').show(3)

### 9 - Spark DataFrame API - sort employee by total income (salary+commission), list name and total income
df_empHeader.withColumn('TOTAL', col('SAL')+col('COMM')).sort(desc('TOTAL')).select('NAME', 'TOTAL').show()

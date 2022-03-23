### Create tables and import data from text files
spark.sql("""
	CREATE TABLE IF NOT EXISTS ken_db.deptHeader(
	DEPT_NO INT, DEPT_NAME STRING, LOC STRING)
	USING CSV
	options ('header' = 'true')
	LOCATION '/data/spark/employee/dept-with-header.txt'
""")
df_dept = spark.read.table('ken_db.deptHeader')

spark.sql("""
	CREATE TABLE IF NOT EXISTS ken_db.emp(
	EMPNO INT, NAME STRING, JOB STRING, MGR INT, HIREDATE TIMESTAMP, SAL INT, COMM INT, DEPTNO INT)
	USING CSV
	LOCATION '/data/spark/employee/emp.txt'
""")
df_emp = spark.read.table('ken_db.emp')

### 1 - Spark SQL - list total salary for each dept
spark.sql("""
	SELECT DEPTNO, sum(SAL) as TOTAL FROM ken_db.emp GROUP BY DEPTNO
""").show()


### 2 - Spark SQL - list total number of employee and average salary for each dept
spark.sql("""
	SELECT DEPTNO, count(*), avg(SAL) FROM ken_db.emp GROUP BY DEPTNO
""").show()

### 3 - Spark SQL - list the first hired employee's name for each dept
spark.sql("""
	SELECT NAME, a.DEPTNO, HIREDATE FROM ken_db.emp JOIN
	(SELECT DEPTNO, min(HIREDATE) as first FROM ken_db.emp GROUP BY DEPTNO) as a
	ON ken_db.emp.HIREDATE = a.first AND ken_db.emp.DEPTNO = a.DEPTNO
""").show()

### 4 - Spark SQL - list total employee salary for each city
spark.sql("""
	SELECT sum(emp.SAL), LOC FROM ken_db.emp
	INNER JOIN ken_db.deptHeader
	on emp.DEPTNO = deptHeader.DEPT_NO
	GROUP BY LOC
""").show()

### 5 - Spark SQL - list employee's name and salary whose salary is higher than their manager
spark.sql("""
	SELECT emp2.NAME, emp2.SAL FROM ken_db.emp
	INNER JOIN ken_db.emp as emp2
	on emp.EMPNO = emp2.MGR
	WHERE emp.SAL < emp2.SAL
""").show()

### 6 - Spark SQL - list employee's name and salary whose salary is higher than average salary of whole company
spark.sql("""SELECT NAME, SAL FROM ken_db.emp JOIN
	(SELECT avg(SAL) as average FROM ken_db.emp) as a
	ON ken_db.emp.SAL > a.average
""").show()

### 7 - Spark SQL - list employee's name and dept name whose name start with "J"
spark.sql("""SELECT NAME, DEPT_NAME FROM ken_db.emp
INNER JOIN ken_db.deptHeader
ON emp.DEPTNO = deptHeader.DEPT_NO
WHERE NAME LIKE "J%"
""").show()

### 8 - Spark SQL - list 3 employee's name and salary with highest salary
spark.sql("""
	SELECT NAME, SAL FROM ken_db.emp ORDER BY SAL DESC LIMIT 3
""").show()

### 9 - Spark SQL - sort employee by total income (salary+commission), list name and total income
spark.sql("""
	SELECT NAME, (SAL+COMM) AS TOTAL FROM ken_db.emp ORDER BY TOTAL DESC
""").show()

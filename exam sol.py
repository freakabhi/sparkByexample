## find max store sales
>>> str = sc.textFile("/user/cloudera/datasets/store_sales_2015.csv")
>>> str_1 = str.map(lambda rec: rec ) 
>>> max_str = str_1.reduce( lambda rec1, rec2 : (rec1 if(float(rec1.split(",")[3]) > float(rec2.split(",")[3])) else rec2 ))
>>> print(max_str)


## find the top sales per Quarter
>>> store_sales = sc.textFile("/user/root/datasets/store_sales/store_sales_2015.csv")
>>> store_p = store_sales.map(lambda x: (x.split(",")[2],x))
>>> store_p1 = store_p.reduceByKey(lambda x,y : max(x,y,key=lambda x: x[-1]))
>>> for i in store_p1.collect():print(i)


>>> store_sales = sc.textFile("/user/root/datasets/store_sales/store_sales_2015.csv")
>>> str_tot = store_sales.map(lambda x: x.split(",")).map(lambda y: (y[0],float(y[3]))).reduceByKey(lambda x,y : x+y).sortBy(lambda a: -a[1])

top 3:
>>> str_tot = store_sales.map(lambda x: x.split(",")).map(lambda y: (y[0],float(y[3]))).reduceByKey(lambda x,y : x+y).sortBy(lambda a: -a[1]).take(3)

>>> st1 = sc.textFile("/user/root/datasets/store_sales/store_sales_2015.csv")
>>> stm = sc.textFile("/user/root/spark_datasets/sqoop_store_master.csv")

>>> st1_p = st1.map(lambda x: (x.split(",")[0],x))
>>> stm_p = stm.map(lambda x: (x.split(",")[0],x))

>>> stj = st1_p.join(stm_p)
>>> stj.take(4)
[(u'AA', (u'AA,2015,1,105000', u'AA,AA Shopping Plaza,Melville,NY')), (u'AA', (u'AA,2015,2,125000', u'AA,AA Shopping Plaza,Melville,NY')), (u'AA', (u'AA,2015,3,145000', u'AA,AA Shopping Plaza,Melville,NY')), (u'AA', (u'AA,2015,4,135000', u'AA,AA Shopping Plaza,Melville,NY'))]
>>> stj1 = stj.map(lambda x: (str(x[0]),x[1][0].split(",")[1],x[1][0].split(",")[2],x[1][0].split(",")[3],x[1][1].split(",")[1],x[1][1].split(",")[2],x[1][1].split(",")[3]))
[('AA', u'2015', u'1', u'105000', u'AA Shopping Plaza', u'Melville', u'NY'), ('AA', u'2015', u'2', u'125000', u'AA Shopping Plaza', u'Melville', u'NY')]


======
pyspark --packages com.databricks:spark-csv_2.11:1.1.0 --master local[4]
pyspark --master local --
>>> airp = sc.textFile("file:///home/cloudera/Downloads/2008.csv")

>> from pyspark.sql import SQLContext
>>> sqc = SQLContext(sc)
>>> df1 = sqc.read.format("com.databricks.spark.csv").option("header","true").load("file:///home/cloudera/Downloads/2008.csv")
>>> df1.take(5)

>>> df1.printSchema()
>>> df1.Year.cast("integer")


>>> df2 = df1.withColumnRenamed("Year","oldYear")
>>> df3 = df2.withColumn("Year",df2["oldYear"].cast("int")).drop("oldYear")
>>> def convertColumn(df, name, new_type):
...     df_1 = df.withColumnRenamed(name, "swap")
...     return df_1.withColumn(name, df_1["swap"].cast(new_type)).drop("swap")
... 
>>> df_3 = convertColumn(df3,"ArrDelay", "int")
>>> df_4 = convertColumn(df_3,"DepDelay", "int")
>>> averageDelays = df_4.groupBy("FlightNum").agg({"ArrDelay":"avg","DepDelay":"avg"})
>>> averageDelays.sort(averageDelays["avg(ArrDelay)"].desc(),"avg(DepDelay)").show(50)

	>>> df.sort(df.age.desc()).collect()
[Row(age=5, name=u'Bob'), Row(age=2, name=u'Alice')]
>>> df.sort("age", ascending=False).collect()
[Row(age=5, name=u'Bob'), Row(age=2, name=u'Alice')]
>>> df.orderBy(df.age.desc()).collect()
[Row(age=5, name=u'Bob'), Row(age=2, name=u'Alice')]
>>> from pyspark.sql.functions import *
>>> df.sort(asc("age")).collect()
[Row(age=2, name=u'Alice'), Row(age=5, name=u'Bob')]
>>> df.orderBy(desc("age"), "name").collect()

[Row(age=5, name=u'Bob'), Row(age=2, name=u'Alice')]
>>> df.orderBy(["age", "name"], ascending=[0, 1]).collect()
[Row(age=5, name=u'Bob'), Row(age=2, name=u'Alice')]
*************
--packages com.databricks:spark-avro_2.10:2.0.1

>>> df.registerTempTable("pokemon")
>>> t1 = sqc.sql("select * from pokemon")



df1 = sqc.read.format("com.databricks.spark.csv").option("header","true").load("file:///mnt/home/abhidocs/Desktop/spark/store_sales_2015.csv")
>>> schemaString = "store_cd year quarter tot_sales"
-----------------------
# apply schema to RDD
# Import SQLContext and data types
from pyspark.sql import SQLContext
from pyspark.sql.types import *

# sc is an existing SparkContext.
sqlContext = SQLContext(sc)

# Load a text file and convert each line to a tuple.
lines = sc.textFile("examples/src/main/resources/people.txt")
parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: (p[0], p[1].strip()))

# The schema is encoded in a string.
schemaString = "name age"

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

# Apply the schema to the RDD.
schemaPeople = sqlContext.createDataFrame(people, schema)

# Register the DataFrame as a table.
schemaPeople.registerTempTable("people")

# SQL can be run over DataFrames that have been registered as a table.
results = sqlContext.sql("SELECT name FROM people")

# The results of SQL queries are RDDs and support all the normal RDD operations.
names = results.map(lambda p: "Name: " + p.name)
for name in names.collect():
  print(name)
  ------------
      str = sc.textFile("/user/cloudera/datasets/store_sales_2015.csv")
>>> str_lkp = sc.textFile("/user/cloudera/datasets/sqoop_store_master.csv")
>>> str_1 = str.map(lambda x:x.split(",")).map(lambda x: (x[0],x[1],x[2],x[3]))
>>> str_lkp_1 = str_lkp.map(lambda x:x.split(",")).map(lambda x: (x[0],x[1]))
>>> sjoin = str_1.join(str_lkp_1)

-----------

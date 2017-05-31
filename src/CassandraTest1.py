from pyspark.sql import SQLContext, Row
from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Cassandra Test App")\
    .set("spark.cassandra.connection.host", "127.0.0.1")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

simple_rdd = sc.parallelize(["This is line1",
                             "This is line2",
                             "This is line3"])

print("The contents of the RDD : ", simple_rdd.take(1))

rdd1 = simple_rdd.map(lambda l: l.split(" "))

print("Changed value of RDD : ", rdd1.take(1))

rdd1_df = rdd1.map(lambda row: Row(col1=row[0], col2=row[1], col3=row[2]))

simple_schema = sqlContext.createDataFrame(rdd1_df)

print("The type of simple_schema : ", type(simple_schema))

simple_schema.select("col1", "col2", "col3").write.\
    format("org.apache.spark.sql.cassandra").\
    options(table="simple", keyspace="training").\
    save(mode="append")

from random import random
from datetime import datetime, timedelta

from pyspark import SparkConf
from pyspark_cassandra import context


spark_conf = SparkConf().setAppName("Cassandra Test App")\
						.set("spark.cassandra.connection.host", "127.0.0.1")
csc = context.CassandraSparkContext(conf=spark_conf)

rdd = csc.parallelize([{
    "key": k,
    "stamp": datetime.now(),
	"val": random() * 10,
	"tags": ["a", "b", "c"],
	"options": {
	"foo": "bar",
	"baz": "qux",
	}
} for k in ["x", "y", "z"]])

print("Type of RDD : ", type(rdd))
print("The lines of RDD : ", rdd.collect())

rdd.saveToCassandra("keyspace", "table", ttl=timedelta(hours=1))
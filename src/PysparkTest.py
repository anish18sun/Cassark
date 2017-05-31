from pyspark import SparkContext, SparkConf

spark_conf = SparkConf().setAppName("scavenge some logs")
spark_context = SparkContext(conf=spark_conf)
address = "/home/anish/file.txt"
log = spark_context.textFile(address)

my_result = log.filter(lambda x: 'foo' in x).saveAsTextFile('/home/anish/file.txt')
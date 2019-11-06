from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext('local[2]', 'NetworkWordCount')
ssc = StreamingContext(sc,2)

lines = ssc.socketTextStream('localhost',3333)

words = lines.flatMap(lambda l: l.split())

pairs = words.map(lambda w:(w,1))
wordsCounts = pairs.reduceByKey(lambda x,y: x+y)

wordsCounts.pprint()

ssc.awaitTermination()

import os

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


from collections import namedtuple

os.environ["SPARK_HOME"] = "C:\\spark-2.4.3-bin-hadoop2.7"
os.environ["HADOOP_HOME"]="C:\\winutils"

def main():
    sc = SparkContext(appName="PysparkStreamingWordCount")

    ssc = StreamingContext(sc, 5)
    lines = ssc.socketTextStream("localhost", 1235)
    fields = ("word", "count")
    Tweet = namedtuple('Text', fields)

    counts = lines.flatMap(lambda text: text.split(" "))\
        .map(lambda x: (x, 1))\
        .reduceByKey(lambda a, b: a + b).map(lambda rec: Tweet(rec[0], rec[1]))
    counts.pprint()
    ssc.start()
    ssc.awaitTermination()

if __name__ == "__main__":
    main()
import sys
import os

os.environ["SPARK_HOME"] = "C:\\spark-2.3.3-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "C:\\winutils"

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def main():
    sc = SparkContext(appName="PysparkStreaming")
    ssc = StreamingContext(sc, 3)   #Streaming will execute in each 3 seconds
    lines = ssc.textFileStream('log')  #'log/ mean directory name
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda a, b: a + b)
    counts.pprint()
    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
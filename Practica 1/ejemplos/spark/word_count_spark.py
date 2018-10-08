# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
import sys


def main():
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    
    lines = sc.textFile(sys.argv[1])
    counts = (lines.flatMap(lambda x: x.split())
              .map(lambda x: (x, 1))
              .reduceByKey(lambda x,y: x + y)
             )
    output = counts.collect()
    print(output)
    sc.stop()


if __name__ == "__main__":
    main()

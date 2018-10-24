'''JoseJavierCortesTejada y AitorCayonRuano declaramos que esta solución
es fruto exclusivamente de nuestro trabajo personal. No hemos sido
ayudados por ninguna otra persona ni hemos obtenido la solución de
fuentes externas, y tampoco hemos compartido nuestra solución con
nadie. Declaramos además que no hemos realizado de manera desho-
nesta ninguna otra actividad que pueda mejorar nuestros resultados
ni perjudicar los resultados de los demás.'''

import sys
from termcolor import colored
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName('Simpsons 4') \
    .config('spark.some.config.option', 'some-value') \
    .getOrCreate()

schemaString = 'word happiness_rank happiness_average happiness_standard_deviation twitter_rank google_rank nyt_rank lyrics_rank'

fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

happiness = spark.read \
                    .format('csv') \
                    .schema(schema) \
                    .option('header', 'false') \
                    .option("delimiter", "\t") \
                    .load('happiness.txt') \
                    .select('word', 'happiness_average')



happiness_dict = dict((happiness.rdd
                        .map(lambda x: {x[0],x[1]}).collect()))

broadcast_dict = spark.sparkContext.broadcast(happiness_dict)

sqlContext = SQLContext(spark.sparkContext)

def happiness_line(s):
    acc = 0.0
    for x in s.split():
        if broadcast_dict.value.get(x) != None:
            acc += float(broadcast_dict.value.get(x))
    return acc

sqlContext.udf.register("happy", happiness_line, FloatType())

simpsons_episodes = spark.read \
                .format('csv') \
                .option('inferSchema', 'true') \
                .option('header', 'true') \
                .load('simpsons_episodes.csv') \
                .select('id', 'imdb_rating')

simpsons_script_lines = spark.read \
                .format('csv') \
                .option('inferSchema', 'true') \
                .option('header', 'true') \
                .load('simpsons_script_lines.csv') \
                .select('episode_id', 'normalized_text') \
                .filter("normalized_text is not null")

simpsons_episodes.createOrReplaceTempView("episode_view")
simpsons_script_lines.createOrReplaceTempView("lines_view")

sentiment = spark.sql("""
    SELECT l.episode_id, e.imdb_rating, ROUND(SUM(happy(l.normalized_text)), 2) AS happy_count
    FROM episode_view e
    JOIN lines_view l ON e.id = l.episode_id
    GROUP BY l.episode_id, e.imdb_rating
    """)

sentiment.sort(asc('episode_id')).show(100)

print(sentiment.stat.corr("imdb_rating", "happy_count", "pearson"))
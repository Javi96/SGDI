'''JoseJavierCortesTejada y AitorCayonRuano declaramos que esta solución
es fruto exclusivamente de nuestro trabajo personal. No hemos sido
ayudados por ninguna otra persona ni hemos obtenido la solución de
fuentes externas, y tampoco hemos compartido nuestra solución con
nadie. Declaramos además que no hemos realizado de manera desho-
nesta ninguna otra actividad que pueda mejorar nuestros resultados
ni perjudicar los resultados de los demás.'''
import sys
from termcolor import colored

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName('Simpsons 3') \
    .config('spark.some.config.option', 'some-value') \
    .getOrCreate()

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
				.select('episode_id', 'raw_text', 'word_count') 


simpsons_episodes.createOrReplaceTempView("episodes")
simpsons_script_lines.createOrReplaceTempView("lines")

text = spark.sql("""
	SELECT l.episode_id, count(raw_text) AS dialogs, sum(word_count) AS words
	FROM lines l
	GROUP BY l.episode_id
	""")

text.createOrReplaceTempView("text_view")

script = spark.sql("""
	SELECT t.episode_id, e.imdb_rating, t.dialogs, t.words
	FROM text_view t
	JOIN episodes e ON e.id = t.episode_id
	""")

#script.sort(asc('episode_id')).show(100)

print(script.stat.corr("imdb_rating", "dialogs", "pearson"))
print(script.stat.corr("imdb_rating", "words", "pearson"))

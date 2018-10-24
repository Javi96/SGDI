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
    .appName('Simpsons 2') \
    .config('spark.some.config.option', 'some-value') \
    .getOrCreate()

simpsons_episodes = spark.read \
				.format('csv') \
				.option('inferSchema', 'true') \
				.option('header', 'true') \
				.load('simpsons_episodes.csv') \
				.select('id', 'imdb_rating')

simpsons_characters = spark.read \
				.format('csv') \
				.option('inferSchema', 'true') \
				.option('header', 'true') \
				.load('simpsons_characters.csv') \
				.select('id', 'gender')

simpsons_script_lines = spark.read \
				.format('csv') \
				.option('inferSchema', 'true') \
				.option('header', 'true') \
				.load('simpsons_script_lines.csv') \
				.select('episode_id', 'character_id') \
				.dropDuplicates(['episode_id', 'character_id']) # un mismo personaje puede tener varias lineas de dialogo en un episodio, es necesario

simpsons_episodes.createOrReplaceTempView("episodes")
simpsons_characters.createOrReplaceTempView("characters")
simpsons_script_lines.createOrReplaceTempView("lines")

count_masc = spark.sql("""
	SELECT l.episode_id, count(*) as masc
	FROM characters c
	JOIN lines l ON l.character_id = c.id
	WHERE c.gender == "m"
	GROUP BY l.episode_id
	""")

'''count_masc.sort(asc('episode_id')).show(100)'''

count_fem = spark.sql("""
	SELECT l.episode_id, count(*) as fem
	FROM characters c
	JOIN lines l ON l.character_id = c.id
	WHERE c.gender == "f"
	GROUP BY l.episode_id
	""")

'''count_fem.sort(asc('episode_id')).show(100)'''

count_total = spark.sql("""
	SELECT l.episode_id, count(*) as total
	FROM characters c
	JOIN lines l ON l.character_id = c.id
	GROUP BY l.episode_id
	""")

'''count_total.sort(asc('episode_id')).show(100)'''

count_total.createOrReplaceTempView("total_view")
count_masc.createOrReplaceTempView("masc_view")
count_fem.createOrReplaceTempView("fem_view")

characters = spark.sql("""
	SELECT e.id, e.imdb_rating, t.total, f.fem, m.masc
	FROM episodes e
	JOIN total_view t ON e.id = t.episode_id
	JOIN masc_view m ON e.id = m.episode_id
	JOIN fem_view f ON e.id = f.episode_id
	""")

characters.sort(asc('id')).show(50)

print(characters.stat.corr("imdb_rating", "total", "pearson"))
print(characters.stat.corr("imdb_rating", "fem", "pearson"))
print(characters.stat.corr("imdb_rating", "masc", "pearson"))

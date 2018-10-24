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
    .appName('Simpsons 1') \
    .config('spark.some.config.option', 'some-value') \
    .getOrCreate()

simpsons_episodes = spark.read \
				.format('csv') \
				.option('inferSchema', 'true') \
				.option('header', 'true') \
				.load('simpsons_episodes.csv') \
				.select('id', 'imdb_rating')

# solo hay que eliminar duplicados aqui, si se dejan la tabla tiene
# mas entradas innecesarias al estar repetidas
simpsons_script_lines = spark.read \
				.format('csv') \
				.option('inferSchema', 'true') \
				.option('header', 'true') \
				.load('simpsons_script_lines.csv') \
				.select('episode_id', 'location_id') \
				.dropDuplicates(['episode_id', 'location_id'])

simpsons_script_lines.createOrReplaceTempView("main")
simpsons_episodes.createOrReplaceTempView("episodes")

locations = spark.sql("""
	SELECT DISTINCT m.episode_id, e.imdb_rating, COUNT(m.location_id) AS places
	FROM main m
	JOIN episodes e ON m.episode_id = e.id
	group by m.episode_id, e.imdb_rating
	""")

locations.sort(asc('episode_id')).show(20)

print('correlacion de pearson (imdb_rating/locations): ', locations.stat.corr("imdb_rating", "places", "pearson"))
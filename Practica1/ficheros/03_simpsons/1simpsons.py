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

simpsons_locations = spark.read \
				.format('csv') \
				.option('inferSchema', 'true') \
				.option('header', 'true') \
				.load('simpsons_locations.csv') \
				.select('id', 'normalized_name')

# solo hay que eliminar duplicados aqui, si se dejan la tabla tiene
# mas entradas innecesarias al estar repetidas
simpsons_script_lines = spark.read \
				.format('csv') \
				.option('inferSchema', 'true') \
				.option('header', 'true') \
				.load('simpsons_script_lines.csv') \
				.select('episode_id', 'location_id') \
				.dropDuplicates(['episode_id', 'location_id'])

'''simpsons_episodes.sort(asc('id')).show(50)
simpsons_locations.sort(asc('id')).show(50)
'''
#simpsons_script_lines.sort(asc('episode_id')).show(50)


simpsons_script_lines.createOrReplaceTempView("main")
simpsons_episodes.createOrReplaceTempView("episodes")
simpsons_locations.createOrReplaceTempView("locations")

tmp = spark.sql("""
	SELECT m.episode_id, e.imdb_rating, l.normalized_name
	FROM main m
	JOIN episodes e ON m.episode_id = e.id
	JOIN locations l ON m.location_id = l.id
	""")
tmp.sort(asc('episode_id')).show()
#tmp.sort(asc('episode_id')).show(50)
#tmp = spark.sql("SELECT * FROM")
#spark.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()

#simpsons_episodes.printSchema()
#simpsons_locations.printSchema()
#simpsons_script_lines.printSchema()

#tmp.sort(asc('location_id')).show(50)
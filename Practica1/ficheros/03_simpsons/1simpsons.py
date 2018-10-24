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

simpsons_script_lines.createOrReplaceTempView("main")
simpsons_episodes.createOrReplaceTempView("episodes")
simpsons_locations.createOrReplaceTempView("location")

locations = spark.sql("""
	SELECT DISTINCT m.episode_id, e.imdb_rating, COUNT(l.normalized_name) AS places
	FROM main m
	JOIN episodes e ON m.episode_id = e.id
	JOIN location l ON m.location_id = l.id
	group by m.episode_id, e.imdb_rating
	""")

locations.sort(asc('episode_id')).show()

print(locations.stat.corr("imdb_rating", "places", "pearson"))

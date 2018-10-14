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



'''
Construye un DataFrame script con 4 columnas: identificador de
episodio, puntuación IMDB, número total de palabras que aparecen en los
diálogos del episodio y el número total de diálogos en el episodio.
'''

simpsons_episodes = spark.read \
				.format('csv') \
				.option('inferSchema', 'true') \
				.option('header', 'true') \
				.load('simpsons_episodes.csv') \
				.select('id', 'imdb_rating')

# solo hay que eliminar duplicados aqui, si se dejan la tabla tiene
# mas entradas innecesarias al estar repetidas

#  id	episode_id	number	raw_text	timestamp_in_ms	speaking_line	
#  character_id	location_id	raw_character_text	raw_location_text	
#  spoken_words	normalized_text	word_count

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

res = spark.sql("""
	SELECT t.episode_id, e.imdb_rating, t.dialogs, t.words
	FROM text_view t
	JOIN episodes e ON e.id = t.episode_id
	""")

res.sort(asc('episode_id')).show(100)
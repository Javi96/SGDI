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

# esto sobra
tmp = spark.sql("""
	SELECT DISTINCT l.episode_id, e.imdb_rating
	FROM episodes e
	JOIN lines l ON l.episode_id = e.id
	
	""")

tmp.createOrReplaceTempView("tmp_view")

characters = spark.sql("""
	SELECT tmp.episode_id, tmp.imdb_rating, t.total, f.fem, m.masc
	FROM tmp_view tmp
	JOIN total_view t ON tmp.episode_id = t.episode_id
	JOIN masc_view m ON tmp.episode_id = m.episode_id
	JOIN fem_view f ON tmp.episode_id = f.episode_id
	""")

#characters.sort(asc('episode_id')).show(50)

print(characters.stat.corr("imdb_rating", "total", "pearson"))
print(characters.stat.corr("imdb_rating", "fem", "pearson"))
print(characters.stat.corr("imdb_rating", "masc", "pearson"))

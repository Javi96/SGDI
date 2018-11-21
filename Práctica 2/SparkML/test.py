from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark import *
from pyspark.sql.functions import *
import matplotlib.pyplot as plt
import json
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer

from pyspark.sql import SQLContext

from pyspark.sql.types import *

from pyspark.sql.functions import *

from pyspark.ml.feature import StringIndexer
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.classification import LinearSVC
from pyspark.ml.classification import NaiveBayes

spark = SparkSession \
        .builder \
        .getOrCreate()

#---------------------------------------------------------- CARGAMOS LOS DATAFRAMES ----------------------------------------------------------

'''
df = spark.read \
        .format('csv') \
        .option('inferSchema', 'true') \
        .option('header', 'true') \
        .load('adult.test') \
        .drop('education-num') \
        .drop('fnlwgt') \
        .drop('marital-status') \
        .drop('relationship') \
        .withColumnRenamed('capital-gain', 'gain') \
        .withColumnRenamed('hours-per-week', 'hours') \
        .withColumnRenamed('native-country', 'country') \
        .withColumnRenamed('capital-loss', 'loss')\
'''

'''
	Cargamos los dataframes de entrenamiento y testeo, eliminando aquellas columnas que no utilizaremos para entrenar el clasificador
'''

train = spark.read \
        .format('csv') \
        .option('inferSchema', 'true') \
        .option('header', 'true') \
        .load('adult.test') \
        .drop('education-num') \
        .drop('fnlwgt') \
        .drop('marital-status') \
        .drop('relationship') \
        .withColumnRenamed('capital-gain', 'gain') \
        .withColumnRenamed('hours-per-week', 'hours') \
        .withColumnRenamed('native-country', 'country') \
        .withColumnRenamed('capital-loss', 'loss')\

test = spark.read \
        .format('csv') \
        .option('inferSchema', 'true') \
        .option('header', 'true') \
        .load('adult.test') \
        .drop('education-num') \
        .drop('fnlwgt') \
        .drop('marital-status') \
        .drop('relationship') \
        .withColumnRenamed('capital-gain', 'gain') \
        .withColumnRenamed('hours-per-week', 'hours') \
        .withColumnRenamed('native-country', 'country') \
        .withColumnRenamed('capital-loss', 'loss')\

#train.show(30)
#train.printSchema()

#---------------------------------------------------------- FILTRAMOS LAS FILAS INVALIDAS ----------------------------------------------------------

'''
	Filtramos el contenido del dataframe eliminando las entradas incompletas, aquellas con valor ? en alguno de sus atributos
'''

columns = ['workclass','education','occupation','race','sex','country']
for column in columns:
	train = train.filter(train[column] != '?') 
	test = test.filter(test[column] != '?')

train.show(30)

#---------------------------------------------------------- CASTEAMOS LOS ATRIBUTOS CATEGORICOS ----------------------------------------------------------
#----------------------------------------------------------      Y SELECCIONAMOS COLUMNAS       ----------------------------------------------------------

'''
	Transformadores para traducir los atributos categoricos y la clase en valores continuos.
	Para su utilizacion en los clasificadores, por defecto, la columna de la clase debe llamarse "label".
'''
indexers = [StringIndexer(inputCol=column, outputCol=column+"_index") for column in columns]
label = [StringIndexer(inputCol="class", outputCol="label")]
'''
	Ensamblador para combinar todos los atributos en una sola columna para su utilizacion en los clasificadores.
	Por defecto esa columna debe llamarse "features"
'''
assembler = [VectorAssembler(inputCols=['age','education_index','race_index','sex_index','gain','loss','hours'], outputCol='features')]
'''
	Combinamos todos los transformadores en una unica lista.
'''
categorical_cast = indexers + label + assembler

'''
	Construimos el pipeline y procesamos los dataframes de entrenamiento y testeo.
	Cada uno de los transformadores de la lista se corresponde con una etapa del pipeline.
'''
pipeline = Pipeline(stages=categorical_cast)
train = pipeline.fit(train).transform(train)
test = pipeline.fit(test).transform(test)

'''
	Seleccionamos para cada dataframe las columnas requeridas por los clasificadores
'''
final_train = train.select('features','label')
final_test = test.select('features','label')

final_train.show(30)

#---------------------------------------------------------- CONSTRUIMOS LOS MODELOS ----------------------------------------------------------

'''
	Creamos un evaluador para analizar los resultados obtenidos en la clasificacion.
'''
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                              metricName="accuracy")

'''
	Creamos las instancias de los clasificadores.
'''
lr = LogisticRegression(maxIter=10, regParam=0.01)
dt = DecisionTreeClassifier(labelCol="label", featuresCol="features")
rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=10)
gbt = GBTClassifier(labelCol="label", featuresCol="features", maxIter=10)
lsvc = LinearSVC(maxIter=10, regParam=0.1)
nb = NaiveBayes(smoothing=1.0, modelType="multinomial")

classifiers = [lr, dt, rf, gbt, lsvc, nb]
names = ['LogisticRegression', 'DecisionTreeClassifier', 'RandomForestClassifier',
		'GBTClassifier', 'LinearSVC', 'NaiveBayes']

'''
	Iteramos la lista de clasificadores creando en cada iteracion un pipeline con un clasificador.
'''
for classifier, name in zip(classifiers, names):
	pipeline = Pipeline(stages=[classifier])
	'''
		Entrenamos el modelo con el dataframe de entrenamiento y clasificamos el dataframe de testeo
	'''
	predictions = pipeline.fit(final_train).transform(final_test)
	#predictions.show()
	'''
		Comprobamos la precision obtenida en la clasificacion
	'''
	accuracy = evaluator.evaluate(predictions)
	print(name + " - Test set accuracy = " + str(accuracy))
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

columns = ['workclass','education','occupation','race','sex','country']
for column in columns:
	train = train.filter(train[column] != '?') 
	test = test.filter(test[column] != '?')

train.show(30)

#---------------------------------------------------------- CASTEAMOS LOS ATRIBUTOS CATEGORICOS ----------------------------------------------------------
#----------------------------------------------------------      Y SELECCIONAMOS COLUMNAS       ----------------------------------------------------------

indexers = [StringIndexer(inputCol=column, outputCol=column+"_index") for column in columns]
label = [StringIndexer(inputCol="class", outputCol="label")]
assembler = [VectorAssembler(inputCols=['age','education_index','race_index','sex_index','gain','loss','hours'], outputCol='features')]
categorical_cast = indexers + label + assembler

pipeline = Pipeline(stages=categorical_cast)
train = pipeline.fit(train).transform(train)
test = pipeline.fit(test).transform(test)

final_train = train.select('features','label')
final_test = test.select('features','label')

final_train.show(30)

#---------------------------------------------------------- CONSTRUIMOS LOS MODELOS ----------------------------------------------------------

evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                              metricName="accuracy")

lr = LogisticRegression(maxIter=10, regParam=0.01)
dt = DecisionTreeClassifier(labelCol="label", featuresCol="features")
rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=10)
gbt = GBTClassifier(labelCol="label", featuresCol="features", maxIter=10)
lsvc = LinearSVC(maxIter=10, regParam=0.1)
nb = NaiveBayes(smoothing=1.0, modelType="multinomial")

classifiers = [lr, dt, rf, gbt, lsvc, nb]
names = ['LogisticRegression', 'DecisionTreeClassifier', 'RandomForestClassifier'
		'GBTClassifier', 'LinearSVC', 'NaiveBayes']

for classifier, name in zip(classifiers, names):
	pipeline = Pipeline(stages=[classifier])
	predictions = pipeline.fit(final_train).transform(final_test)
	#predictions.show()
	accuracy = evaluator.evaluate(predictions)
	print(name + " - Test set accuracy = " + str(accuracy))
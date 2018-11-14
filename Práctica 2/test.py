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

#train.show(30)

#---------------------------------------------------------- CASTEAMOS LOS ATRIBUTOS CATEGORICOS ----------------------------------------------------------

indexers = [StringIndexer(inputCol=column, outputCol=column+"_index") for column in columns]
for ind in indexers:
	#df = ind.fit(df).transform(df)
	train = ind.fit(train).transform(train)
	test = ind.fit(test).transform(test)
for col in columns:
	#df = df.drop(col)
	train = train.drop(col)
	test = test.drop(col)

label = StringIndexer(inputCol="class", outputCol="label")
#df = label.fit(df).transform(df).drop('class')
train = label.fit(train).transform(train).drop('class')
test = label.fit(test).transform(test).drop('class')

#train.show(30)

#---------------------------------------------------------- SELECCIONAMOS LOS ATRIBUTOS ----------------------------------------------------------

select_train = train.select('age','education_index','race_index','sex_index',
    'gain','loss','hours','label')
select_test = test.select('age','education_index','race_index','sex_index',
    'gain','loss','hours','label')

assembler = VectorAssembler(
    inputCols=['age','education_index','race_index','sex_index',
    'gain','loss','hours'],
    outputCol='features')

final_train = assembler.transform(select_train)
final_train = final_train.select('features','label')

final_test = assembler.transform(select_test)
final_test = final_test.select('features','label')

#final_test.show(30)

#---------------------------------------------------------- CONSTRUIMOS LOS MODELOS ----------------------------------------------------------

evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                              metricName="accuracy")

lr = LogisticRegression(maxIter=10, regParam=0.01)
#print("LogisticRegression parameters:\n" + lr.explainParams() + "\n")
model = lr.fit(final_train)
predictions = model.transform(final_test)
predictions.show()
accuracy = evaluator.evaluate(predictions)
print("LogisticRegression - Test set accuracy = " + str(accuracy))


dt = DecisionTreeClassifier(labelCol="label", featuresCol="features")
#print("DecisionTreeClassifier parameters:\n" + dt.explainParams() + "\n")
model = dt.fit(final_train)
predictions = model.transform(final_test)
predictions.show()
accuracy = evaluator.evaluate(predictions)
print("DecisionTreeClassifier - Test set accuracy = " + str(accuracy))

rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=10)
#print("RandomForestClassifier parameters:\n" + rf.explainParams() + "\n")
model = rf.fit(final_train)
predictions = model.transform(final_test)
predictions.show()
accuracy = evaluator.evaluate(predictions)
print("RandomForestClassifier - Test set accuracy = " + str(accuracy))

gbt = GBTClassifier(labelCol="label", featuresCol="features", maxIter=10)
#print("GBTClassifier parameters:\n" + gbt.explainParams() + "\n")
model = gbt.fit(final_train)
predictions = model.transform(final_test)
predictions.show()
accuracy = evaluator.evaluate(predictions)
print("GBTClassifier - Test set accuracy = " + str(accuracy))

lsvc = LinearSVC(maxIter=10, regParam=0.1)
#print("LinearSVC parameters:\n" + lsvc.explainParams() + "\n")
model = lsvc.fit(final_train)
predictions = model.transform(final_test)
predictions.show()
accuracy = evaluator.evaluate(predictions)
print("LinearSVC - Test set accuracy = " + str(accuracy))

nb = NaiveBayes(smoothing=1.0, modelType="multinomial")
#print("NaiveBayes parameters:\n" + nb.explainParams() + "\n")
model = nb.fit(final_train)
predictions = model.transform(final_test)
predictions.show()
accuracy = evaluator.evaluate(predictions)
print("NaiveBayes - Test set accuracy = " + str(accuracy))

'''def cleanup_age():
    limits = [17, 31, 41, 51, 99]
    values = ['17-30', '31-40', '41-50', '51-99']
    result = {}
    for i in range(0, len(limits)-1):
        for age in range(limits[i], limits[i+1]):
            result[str(age)] = values[i]
    return result

df.select('age').show(10)
df_age = df.select('age').replace(cleanup_age())

df_age.select('age').show(10)

def cleanup_gain():
    limits = [1, 1001, 2001, 20001, 100000]
    values = ['1-1000', '1001-2000', '2001-20000', '20001-99999']
    result = {}
    for i in range(0, len(limits)-1):
        for gain in range(limits[i], limits[i+1]):
            result[str(gain)] = values[i]
    return result

df.select('gain').show(10)
df_gain = df.select('gain').replace(cleanup_gain())

df_gain.groupby('gain').count().show(500)'''

'''
cleanup_age_17_30 = udf(lambda age: "17-30" if age >='17' and age <='30' else age, StringType())
cleanup_age_31_40 = udf(lambda age: "31-40" if age >='31' and age <='40' else age, StringType())
cleanup_age_41_50 = udf(lambda age: "41-50" if age >='41' and age <='50' else age, StringType())
cleanup_age_50_99 = udf(lambda age: "51-99" if age >='51' and age <='99' else age, StringType())

df = df.withColumn('age', cleanup_age_17_30(df.age))
df = df.withColumn('age', cleanup_age_31_40(df.age))
df = df.withColumn('age', cleanup_age_41_50(df.age))
df = df.withColumn('age', cleanup_age_50_99(df.age))

# age: continuous
df_age = df.groupby('age').count().sort('age').withColumnRenamed('count', 'age_count')

age = [i.age for i in df_age.select('age').collect()]
age_count = [i.age_count for i in df_age.select('age_count').collect()]

plt.scatter(age, age_count)
plt.show()

# fnlwgt: continuous
df_fnlwgt = df.groupby('fnlwgt').count().sort('fnlwgt').withColumnRenamed('count', 'fnlwgt_count')
df_fnlwgt.show(10)
x = [i.fnlwgt for i in df_fnlwgt.select('fnlwgt').collect()]
y = [i.fnlwgt_count for i in df_fnlwgt.select('fnlwgt_count').collect()]

plt.scatter(x, y)
plt.show()

# capital_gain
df_gain = df.groupby('gain').count().sort('gain').withColumnRenamed('count', 'gain_count')
df_gain.show(200)
x = [i.gain for i in df_gain.select('gain').collect()]
y = [i.gain_count for i in df_gain.select('gain_count').collect()]

plt.scatter(x, y)
plt.show()

# capital_lose
df_loss = df.groupby('loss').count().sort('loss').withColumnRenamed('count', 'loss_count')
df_loss.show(200)
x = [i.loss for i in df_loss.select('loss').collect()]
y = [i.loss_count for i in df_loss.select('loss_count').collect()]

plt.scatter(x, y)
plt.show()

# hours-per-week: continuous
df_hours = df.groupby('hours_per_week').count().sort('hours_per_week').withColumnRenamed('count', 'hours_per_week_count')
df_hours.show(200)
x = [i.hours_per_week for i in df_hours.select('hours_per_week').collect()]
y = [i.hours_per_week_count for i in df_hours.select('hours_per_week_count').collect()]

plt.scatter(x, y)
plt.show()
'''
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


spark = SparkSession \
        .builder \
        .getOrCreate()

df = spark.read \
        .format('csv') \
        .option('inferSchema', 'false') \
        .option('header', 'true') \
        .load('adult.test') \
        .drop('education-num') \
        .withColumnRenamed('capital-gain', 'gain') \
        .withColumnRenamed('hours-per-week', 'hours_per_week') \
        .withColumnRenamed('marital-status', 'marital_status') \
        .withColumnRenamed('native-country', 'native_country') \
        .withColumnRenamed('capital-loss', 'loss')

df.show(1)

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
df = df.withColumn('age', cleanup_age_50_99(df.age))'''



df_age = df.groupby('age').count().sort('age').withColumnRenamed('count', 'age_count')

age = [i.age for i in df_age.select('age').collect()]
age_count = [i.age_count for i in df_age.select('age_count').collect()]

plt.scatter(age, age_count)
plt.show()


ages = [('17','30'),('31','40'),('41','50'),('51','70'),('71','99')]
for age in ages:
    cleanup_age = udf(lambda col: age[0] + '-' + age[1] if col >=age[0] and col <=age[1] else col, StringType())
    df = df.withColumn('age', cleanup_age(df.age))

df.show(20)






# age: continuous




'''



# capital_gain
df_gain = df.groupby('gain').count().sort('gain').withColumnRenamed('count', 'gain_count')
df_gain.show(200)
x = [i.gain for i in df_gain.select('gain').collect()]
y = [i.gain_count for i in df_gain.select('gain_count').collect()]

plt.scatter(x, y)
plt.show()


# fnlwgt: continuous
df_fnlwgt = df.groupby('fnlwgt').count().sort('fnlwgt').withColumnRenamed('count', 'fnlwgt_count')
df_fnlwgt.show(10)
x = [i.fnlwgt for i in df_fnlwgt.select('fnlwgt').collect()]
y = [i.fnlwgt_count for i in df_fnlwgt.select('fnlwgt_count').collect()]

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



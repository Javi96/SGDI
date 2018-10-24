'''JoseJavierCortesTejada y AitorCayonRuano declaramos que esta solución
es fruto exclusivamente de nuestro trabajo personal. No hemos sido
ayudados por ninguna otra persona ni hemos obtenido la solución de
fuentes externas, y tampoco hemos compartido nuestra solución con
nadie. Declaramos además que no hemos realizado de manera desho-
nesta ninguna otra actividad que pueda mejorar nuestros resultados
ni perjudicar los resultados de los demás.'''

from pyspark.sql import SparkSession
import sys
from termcolor import colored

def main():
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    
    lines = sc.textFile(sys.argv[1])

    pairs = (lines.filter(lambda x: 'date-time' not in x)
                    .map(lambda x: (x.split('/')[1] + '/' + x.split('/')[0], float(x.split(',')[-1])))
          )

    max_pairs = (pairs.reduceByKey(lambda x, y: max(x, y)))

    min_pairs = (pairs.reduceByKey(lambda x, y: min(x, y)))

    avg_pairs = (pairs.groupByKey()
                        .map(lambda x: (x[0], len(x[1]), list(x[1])))
                        .map(lambda x: (x[0], x[1], [float(y) for y in x[2]]))
                        .map(lambda x: (x[0], round(sum(x[2])/x[1], 2))))

    max_min_pairs = (max_pairs.join(min_pairs))

    res = (max_min_pairs.join(avg_pairs)
                        .map(lambda x: (x[0], x[1][0][0], x[1][0][1], x[1][1]))
                        .map(lambda x: (x[0], {'max':x[1], 'avg':x[3], 'min':x[2]})))
                                            

    output = res.collect()
    for i in output:
        print(colored(i, 'yellow'))
    sc.stop()


if __name__ == "__main__":
    main()

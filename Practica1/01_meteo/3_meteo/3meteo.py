'''JoseJavierCortesTejada y AitorCayonRuano declaramos que esta solución
es fruto exclusivamente de nuestro trabajo personal. No hemos sido
ayudados por ninguna otra persona ni hemos obtenido la solución de
fuentes externas, y tampoco hemos compartido nuestra solución con
nadie. Declaramos además que no hemos realizado de manera desho-
nesta ninguna otra actividad que pueda mejorar nuestros resultados
ni perjudicar los resultados de los demás.'''

from mrjob.job import MRJob
import json
from termcolor import colored

def process_combiner(values):
    data = next(values)
    dicc = {'max': data[0], 'min': data[1], 'avg': data[2], 'count': data[3]}
    for i in values:
        tmp_max, tmp_min, tmp_avg, tmp_count = i
        if dicc["max"] < tmp_max:
            dicc["max"] = tmp_max
        if dicc["min"] > tmp_min:
            dicc["min"] = tmp_min
        dicc["avg"] += tmp_avg
        dicc["count"] += tmp_count
    return dicc

class MRMeteo(MRJob):

    def mapper(self, key, line):
        if not line.startswith('date-time'): 
            date = line.split('/')[1] + '/' + line.split('/')[0]
            battery = float(line.split(",")[-1])
            yield(date, (battery, battery, battery, 1))

    def combiner(self, key, values):
        dicc = process_combiner(values)
        yield(key, (dicc["max"], dicc["min"], dicc["avg"], dicc["count"]))  # devolver sumatorio y longitud por id

    def reducer(self, key, values): # max min avg
        dicc = process_combiner(values)    
        yield(key, {'max':dicc['max'], 'min':dicc['min'], 'avg':round(dicc['avg']/dicc['count'], 2)})

if __name__ == '__main__':
        MRMeteo.run()
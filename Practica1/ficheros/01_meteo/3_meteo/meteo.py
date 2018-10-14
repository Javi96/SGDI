from mrjob.job import MRJob
import json
from termcolor import colored

def process_data(values):
    data = next(values)
    dicc = {'max': data[0], 'min': data[1], 'avg': data[2]}
    count = 1
    for i in values:
        if dicc["max"] < float(i[0]):
            dicc["max"] = float(i[0])
        if dicc["min"] > float(i[1]):
            dicc["min"] = float(i[1])
        dicc["avg"] += float(i[2])
        count += 1
    dicc["avg"] = round(dicc["avg"]/count, 2)  
    return dicc

class MRMeteo(MRJob):

    def mapper(self, key, line):
        if not line.startswith('date-time'): 
            date = line.split('/')[1] + '/' + line.split('/')[0]
            battery = float(line.split(",")[-1])
            yield(date, (battery, battery, battery))

    def combiner(self, key, values):
        dicc = process_data(values)
        yield(key, (dicc["max"], dicc["min"], dicc["avg"]))


    def reducer(self, key, values): # max min avg
        yield(key, process_data(values))


if __name__ == '__main__':
        MRMeteo.run()
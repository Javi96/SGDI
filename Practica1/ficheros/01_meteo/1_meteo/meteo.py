from mrjob.job import MRJob
import json
from termcolor import colored

class MRMeteo(MRJob):

    def mapper(self, key, line):
        if not line.startswith('date-time'): 
            date = line.split('/')[1] + '/' + line.split('/')[0]
            battery = line.split(",")[-1]
            yield(date, battery)

    def reducer(self, key, values):
        data = float(next(values))
        dicc = {'max': data, 'min': data, 'avg': data}
        count = 1
        for i in values:
            value = float(i)
            if dicc["max"] < value:
                dicc["max"] = value
            if dicc["min"] > value:
                dicc["min"] = value
            dicc["avg"] += value
            count += 1
        dicc["avg"] = round(dicc["avg"]/count, 2)
        yield(key, dicc)


if __name__ == '__main__':
        MRMeteo.run()
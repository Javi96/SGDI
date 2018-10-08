from mrjob.job import MRJob
import json
from termcolor import colored

class MRMeteo(MRJob):

    # Fase MAP (line es una cadena de texto)
    def mapper(self, key, line):
        if line.startswith('date-time'): 
            print(colored(line, 'red'))
        else:
            date = line.split(",")[0].split(" ")[0].split("/") # [2014, 01, 10]
            complete_date = date[1] + "/" + date[0] # 01/2014
            battery = line.split(",")[8]
            yield(complete_date, battery)
            #print(colored(line, 'green'))

    # Fase REDUCE (key es una cadena texto, values un generador de valores)
    def reducer(self, key, values):
        try:
            print(colored(key, 'green'))
            max_battery = float(next(values))
            min_battery = max_battery
            avg_battery = max_battery
            count = 1
            for i in values:
                if max_battery < float(i):
                    max_battery = float(i)
                if min_battery > float(i):
                    min_battery = float(i)
                avg_battery += float(i)
                count += 1
            res = {}
            res["max"] = max_battery
            res["avg"] = round(avg_battery/count, 2)
            res["min"] = min_battery
            yield(key, res)
        except Exception:
            print("error")


if __name__ == '__main__':
        MRMeteo.run()
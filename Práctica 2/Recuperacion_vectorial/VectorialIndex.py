# Insertar aqui la cabecera

import string
import glob
import os
import os.path as os_path
import sys
from termcolor import colored
import json
from subprocess import call
import math
import operator
# Dada una linea de texto, devuelve una lista de palabras no vacias 
# convirtiendo a minusculas y eliminando signos de puntuacion por los extremos
# Ejemplo:
#   > extrae_palabras("Hi! What is your name? John.")
#   ['hi', 'what', 'is', 'your', 'name', 'john']

def extrae_palabras(linea):
    return filter(lambda x: len(x) > 0, 
        map(lambda x: x.lower().strip(string.punctuation), linea.split()))

def get_files(path):
    result = []
    for root, dirs, files in os.walk(path):
        for f in files:
            if f == 'VectorialIndex.py': 
                pass
            else:
                result.append(os_path.relpath(os_path.join(root, f), path))
    return result

def get_files_dict(path):
    result = {}
    files_count = 0
    for root, dirs, files in os.walk(path):
        for f in files:
            if f == 'VectorialIndex.py':
                pass
            else:
                result[files_count] = path + os_path.relpath(os_path.join(root, f), path) 
                files_count += 1
                #result.append(os_path.relpath(os_path.join(root, f), path))

    #print(colored(json.dumps(result, indent=4, sort_keys=True)))
    return result

class VectorialIndex(object):

    def __init__(self, path, stop=[]):

        self.reverse_index = {}
        self.stop = stop
        self.stop.sort()
        self.weigth = {}
        self.files = get_files_dict(path)
        self.create_index()
        self.calculate_tf_ij()
        self.calculate_weigth()

    def check_word(self, word, file):
        if word in self.reverse_index:
            if file in self.reverse_index[word].keys():
                self.reverse_index[word][file] += 1
            else: 
                self.reverse_index[word][file] = 1
        else:
            self.reverse_index[word] = {file: 1}

    def calculate_weigth(self):
        for word in self.reverse_index.items():
            for document in word[1].items():
                if document[0] not in self.weigth.keys():
                    self.weigth[document[0]] = math.pow(document[1], 2)
                else:
                    self.weigth[document[0]] += math.pow(document[1], 2)
        #print('weight: ', self.weigth)
        for document in self.weigth.items():
            #print(document[0], document[1])
            self.weigth[document[0]] = math.sqrt(document[1])
        #print('weight: ', self.weigth)

    def calculate_tf_ij(self):
        for word in self.reverse_index.items():
            for document in word[1].items():
                self.reverse_index[word[0]][document[0]] = (1 + math.log(self.reverse_index[word[0]][document[0]], 2)) * math.log(len(self.files)/len(word[1].keys()), 2)

    def create_index(self):
        for file in self.files.items():
            with open(file[1], 'r', encoding='latin1') as input_file:
                for line in input_file:
                    words = extrae_palabras(str(line))
                    for word in words:
                        if word not in self.stop:
                            self.check_word(word, file[0])

    def consulta_vectorial(self, consulta, n=3):
        scores = {}
        words = extrae_palabras(consulta)
        for word in words:
            #print('word: ', word)
            if word in self.reverse_index.keys():
                #print(self.reverse_index)
                weigths = self.reverse_index[word]
                #print(json.dumps(weigths, indent=4))
                for weigth in weigths.items():
                    if weigth[0] not in scores:
                        scores[weigth[0]] = 0
                    scores[weigth[0]] += weigth[1]
        #print(colored(json.dumps(scores, indent=4), 'blue'))
        for score in scores.keys():
            #print(score, scores[score])
            scores[score] = scores[score]/self.weigth[score]

        #print(colored(json.dumps(scores, indent=4), 'blue'))
        sorted_by_value = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)

        #print(colored(json.dumps(sorted_by_value, indent=4), 'blue'))
        result = [(self.files[res[0]],res[1]) for res in sorted_by_value[0:n]]
        return result
        #print([r for r in result])

       

    def intersect(self, first_set, second_set):
        #print('first_set: ', first_set)
        #print('second_set: ', second_set)
        new_set = []
        while first_set != [] and second_set != []:
            #print(first_set, second_set)
            if first_set[0] == second_set[0]:
                new_set.append(first_set[0])
                #print(colored(new_set, 'red'))
                first_set.pop(0)
                second_set.pop(0)
            elif first_set[0] < second_set[0]:
                first_set.pop(0)
            elif first_set[0] > second_set[0]:
                second_set.pop(0)
            else:
                print('nada')
        return new_set

    def consulta_conjuncion(self, consulta):
        words = extrae_palabras(consulta)
        set_collection = []
        for word in words:
            if word in self.reverse_index.keys():
                print('estoy: ', word)
                set_collection.append(sorted(list(self.reverse_index[word].keys())))
                #print(colored(self.reverse_index[word].keys(), 'yellow'))
        
        while len(set_collection) != 1:
            first_set = set_collection.pop(0)
            second_set = set_collection.pop(0)
            #print(type(first_set), type(second_set))
            set_collection.append(self.intersect(first_set, second_set))

        return set_collection[0]

if __name__ == '__main__':
    call(['clear'])
    vectorialIndex = VectorialIndex(sys.argv[1], ['the', 'an', 'a'])
    res = vectorialIndex.consulta_conjuncion('disease symptoms include')
    print(colored('Conjunction query: ', 'yellow'))
    for r in res:
        print(colored(vectorialIndex.files[r], 'green'))
    print(colored('Vertorial query: ', 'yellow'))
    files_vect = vectorialIndex.consulta_vectorial('disease symptoms include', 3)
    for line in files_vect:
        print(colored((line[0], line[1]), 'green'))
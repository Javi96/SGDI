# Insertar aqui la cabecera

import string
import glob
import os
import os.path as os_path
import sys
from termcolor import colored
import json
# Dada una linea de texto, devuelve una lista de palabras no vacias 
# convirtiendo a minusculas y eliminando signos de puntuacion por los extremos
# Ejemplo:
#   > extrae_palabras("Hi! What is your name? John.")
#   ['hi', 'what', 'is', 'your', 'name', 'john']

def extrae_palabras(linea):
    return filter(lambda x: len(x) > 0, 
        map(lambda x: x.lower().strip(string.punctuation), linea.split()))

class VectorialIndex(object):



    def __init__(self, path, stop=[]):
        self.files = []
        self.reverse_index = {}
        self.stop = stop
        self.stop.sort()
        for root, dirs, files in os.walk(path):
            for f in files:
                if f == 'VectorialIndex.py': 
                    pass
                else:
                    self.files.append(os_path.relpath(os_path.join(root, f), path))
        print(self.files)
        self.create_index()



    def check_word(self, word, file):
        file_name = file.split('/')[-1]
        if word in self.reverse_index:
            if file_name in self.reverse_index[word].keys():
                self.reverse_index[word][file_name] += 1
            else: 
                self.reverse_index[word][file_name] = 1
         
        else:
            self.reverse_index[word] = {file_name: 1}



    def create_index(self):
        for file in self.files:
            with open(file, 'r', encoding='utf8') as input_file:
                for line in input_file:
                    words = extrae_palabras(str(line))
                    for word in words:
                        if word not in self.stop:
                            self.check_word(word, file)

        print(json.dumps(self.reverse_index, indent=4, sort_keys=True))
        print(self.stop)
        
    def consulta_vectorial(self, consulta, n=3):
        pass

    def consulta_conjuncion(self, consulta):
        pass


if __name__ == '__main__':
    vectorialIndex = VectorialIndex(sys.argv[1], ['the', 'an', 'a'])
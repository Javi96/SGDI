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
            if f == 'CompleteIndex.py':
                pass
            else:
                result.append(os_path.relpath(os_path.join(root, f), path))
    return result

def get_files_dict(path):
    result = {}
    files_count = 0
    for root, dirs, files in os.walk(path):
        for f in files:
            if f == 'CompleteIndex.py':
                pass
            else:
                result[files_count] = os_path.relpath(os_path.join(root, f), path) 
                files_count += 1
                #result.append(os_path.relpath(os_path.join(root, f), path))

    print(colored(json.dumps(result, indent=4, sort_keys=True)))
    return result

class CompleteIndex(object):

    def __init__(self, path, compresion=None):
        self.files = get_files_dict(path)

       

    def create_complex_index(self):
        pass

    def consulta_frase(self, frase):
        pass

    def num_bits(self):
        pass


if __name__ == '__main__':
    call(['clear'])
    vectorialIndex = CompleteIndex(sys.argv[1])

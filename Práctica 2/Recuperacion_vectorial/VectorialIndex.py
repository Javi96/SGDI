'''JoseJavierCortesTejada y AitorCayonRuano declaramos que esta solución
es fruto exclusivamente de nuestro trabajo personal. No hemos sido
ayudados por ninguna otra persona ni hemos obtenido la solución de
fuentes externas, y tampoco hemos compartido nuestra solución con
nadie. Declaramos además que no hemos realizado de manera desho-
nesta ninguna otra actividad que pueda mejorar nuestros resultados
ni perjudicar los resultados de los demás.'''

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

def get_files_dict(path):

    result = {}
    files_count = 0
    for root, dirs, files in os.walk(path):
        for f in files:
            result[files_count] = path + os_path.relpath(os_path.join(root, f), path) 
            files_count += 1
    return result

class VectorialIndex(object):

    def __init__(self, path, stop=[]):
        self.reverse_index = {}
        self.weigth = {}
        self.files = get_files_dict(path)
        self.create_index(stop)
        self.calculate_tf_ij()
        self.calculate_weigth()

    def check_word(self, word, file):
        """
        Indexa las palabras en el diccionario.

        Parametros
        ----------
        word : str
            Palabra a insertar
        file: str
            Documento en el que aparece

        """

        if word in self.reverse_index:
            if file in self.reverse_index[word].keys():
                self.reverse_index[word][file] += 1
            else: 
                self.reverse_index[word][file] = 1
        else:
            self.reverse_index[word] = {file: 1}

    def calculate_weigth(self):
        """
        Calcula los pesos del indice

        """

        for word in self.reverse_index.items():
            for document in word[1].items():
                if document[0] not in self.weigth.keys():
                    self.weigth[document[0]] = math.pow(document[1], 2)
                else:
                    self.weigth[document[0]] += math.pow(document[1], 2)
        for document in self.weigth.items():
            self.weigth[document[0]] = math.sqrt(document[1])

    def calculate_tf_ij(self):
        """
        Calcula la tasa tf_idf.

        """

        for word in self.reverse_index.items():
            for document in word[1].items():
                self.reverse_index[word[0]][document[0]] = (1 + math.log(self.reverse_index[word[0]][document[0]], 2)) * math.log(len(self.files)/len(word[1].keys()), 2)

    def create_index(self, stop):
        """
        Crea el indice invertido.

        Parametros
        ----------
        stop : list
            Conjunto de palabras a ignorar

        """

        for file in self.files.items():
            with open(file[1], 'r', encoding='latin1') as input_file:
                for line in input_file:
                    words = extrae_palabras(str(line))
                    for word in words:
                        if word not in stop:
                            self.check_word(word, file[0])

    def consulta_vectorial(self, consulta, n=3):
        """
        Ejecuta una consulta vectorial sobre el indice.

        Parametros
        ----------
        consulta : string
            Consulta a ejecutar
        n : int
            Numero de resultados a devolver

        Retorno
        -------
        list
            N resultados mas relevantes (si hay menos de N se devuelven todos)
        """

        scores = {}
        words = extrae_palabras(consulta)
        for word in words:
            if word in self.reverse_index.keys():
                weigths = self.reverse_index[word]
                for weigth in weigths.items():
                    if weigth[0] not in scores:
                        scores[weigth[0]] = 0
                    scores[weigth[0]] += weigth[1]
        for score in scores.keys():
            scores[score] = scores[score]/self.weigth[score]

        sorted_by_value = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)

        result = [(self.files[res[0]],res[1]) for res in sorted_by_value[0:n]]
        return result

       

    def intersect(self, first_set, second_set):
        """
        Interseca dos listas.

        Parametros
        ----------
        fist_set : list
            Lista 1
        second_set : list
            Lista 2

        Retorno
        -------
        list
            Resultado de la interseccion 
        """

        new_set = []
        while first_set != [] and second_set != []:
            if first_set[0] == second_set[0]:
                new_set.append(first_set[0])
                first_set.pop(0)
                second_set.pop(0)
            elif first_set[0] < second_set[0]:
                first_set.pop(0)
            elif first_set[0] > second_set[0]:
                second_set.pop(0)
        return new_set

    def consulta_conjuncion(self, consulta):
        """
        Ejecuta una consulta conjuncion.

        Parametros
        ----------
        consulta : word
            Consulta a evaluar

        Retorno
        -------
        list
            Lista de respuestas para la consulta 
        """

        words = extrae_palabras(consulta)
        set_collection = []
        for word in words:
            if word in self.reverse_index.keys():
                set_collection.append(sorted(list(self.reverse_index[word].keys())))

        set_collection.sort(key = lambda s: len(s))

        while len(set_collection) != 1:
            first_set = set_collection.pop(0)
            second_set = set_collection.pop(0)
            set_collection = [self.intersect(first_set, second_set)] + set_collection

        return set_collection[0]

if __name__ == '__main__':
    call(['clear'])
    vectorialIndex = VectorialIndex(sys.argv[1], [])
    print(vectorialIndex.reverse_index['duo'])
    res = vectorialIndex.consulta_conjuncion('Duo Dock')
    print(colored('Conjunction query: ', 'yellow'))
    for r in res:
        print(colored(vectorialIndex.files[r], 'green'))
    print(colored('Vertorial query: ', 'yellow'))
    files_vect = vectorialIndex.consulta_vectorial('Duo Dock', 5)
    for line in files_vect:
        print(colored((line[0], line[1]), 'green'))

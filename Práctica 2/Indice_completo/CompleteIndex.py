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
        self.complete_index = self.create_complete_index()
       
    def create_complete_index(self):
        result = {}
        for file in self.files.items():
            with open(file[1], 'r', encoding='utf8') as input_file:
                result = self.get_words(input_file, result, file[0])
        return result
        #print(json.dumps(result, indent=4))

    def get_words(self, input_file, res, file):
        word_count = 0
        for line in input_file:
            words = extrae_palabras(line)
            for word in words:
                word_count += 1
                if word in res.keys():
                    for i in res[word].keys():
                        if file in res[word][i].keys():
                            word_list = res[word][i][file][1]
                            word_list += [word_count]
                            aux = (len(word_list), word_list)
                            res[word][i][file] = aux
                        else:
                            res[word][i][file] = (1,[word_count])
                            aux = res[word][i]
                            res[word] = {i+1:aux}

                else:
                    res[word] = {1:{file:(1,[word_count])}}
        return res
        #print(json.dumps(res, indent=4))
        
    def get_documents(self, words):
        result = []
        count = 0
        keys = self.complete_index.keys()
        for word in words:
            count += 1
            if word in keys:
                for i in self.complete_index[word].keys():
                    aux = self.complete_index[word][i]
                    print(word, ' --- ', aux)
                    print(list(aux.items()))
                    result.append(list(aux.items()))
        for i in result:
            print(i)
        return result, count

    def same_doc_id(self, documents):
        result = True
        aux = documents[0].copy()
        min_doc_id = aux[0][0]
        for document in documents[1:]:
            print(aux[0], '  vs  ', document[0])
            if document[0][0] < min_doc_id:
                min_doc_id = document[0][0]
            if aux[0][0] != document[0][0]:
                result = False
        return result, min_doc_id

    # modificar para todas las ocurrencias en fichero
    def consecutive(self, documents, length): 
        aux = documents[0][0][1][1].copy()
        count = 1
        print(aux)
        for document in documents[1:]:
            print(document[0][1][1])
            aux += document[0][1][1]
        print(aux)
        begin = aux[0]
        for index in aux[1:]:
            print('data: ', begin, index, index+1)
            if begin+1 == index:
                begin = index
                count += 1
                if count == length:
                    return True, document[0][0]
        return False, -1

    def advance_min(self, documents, min_doc_id):
        for document in documents:
            print(document)
            if document[0][0] == min_doc_id:
                print('match: ', document[0][0], min_doc_id)
                document.pop(0)
                if document == []:
                    return False
        return True

    # precondicion: todas las listas tienen al menos un elemento, sino salimos
    # por construccion sabemos que si la frase esta en las listas n1 n1, nn, podemos
    # asegurar que las palabras están en el mismo orden
    def intersect(self, documents, length):
        cont = True
        answer = {}
        while cont:
            result, min_doc_id = self.same_doc_id(documents)
            if result:
                result, file = self.consecutive(documents, length)
                print('ok: ', file)
                if result:
                    answer[file] = self.files[file]
            print(colored(documents, 'yellow'), min_doc_id)
            cont = self.advance_min(documents, min_doc_id)
        return answer

    def consulta_frase(self, frase):
        words = extrae_palabras(frase)
        documents, count = self.get_documents(words)
        result = self.intersect(documents, count)
        print(json.dumps(result, indent=4, sort_keys=True))

    def num_bits(self):
        pass


if __name__ == '__main__':
    call(['clear'])
    vectorialIndex = CompleteIndex(sys.argv[1])
    vectorialIndex.consulta_frase('hola como estas')
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
from bitarray import bitarray
import struct
import Compresion

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
            if f == 'CompleteIndex.py' or f == 'Compresion.py':
                pass
            else:
                result[files_count] = path + os_path.relpath(os_path.join(root, f), path) 
                #print(path + os_path.relpath(os_path.join(root, f), path))
                files_count += 1
                #result.append(os_path.relpath(os_path.join(root, f), path))

    #print(colored(json.dumps(result, indent=4, sort_keys=True)))
    return result



class CompleteIndex(object):

    def __init__(self, path, compresion = None):
        self.compresion = compresion
        self.files = get_files_dict(path)
        self.complete_index = self.create_complete_index()

    def create_complete_index(self):
        result = {}
        for file in self.files.items():
            with open(file[1], 'r', encoding='latin1') as input_file:
                self.get_words(input_file, result, file[0])
        #print(json.dumps(result, indent=4))
        result = getattr(Compresion, 'apply_default')(result)
        #print(result)
        result = getattr(Compresion, self.compresion)(result)
        #print('hiiiiiii')
        #print(result)
        return result
        #print(json.dumps(result, indent=4))

    def get_words(self, input_file, res, file):
        word_count = 0
        for line in input_file:
            words = extrae_palabras(line)
            for word in words:
                word_count += 1
                if word in res.keys():
                    doc_id = list(res[word].keys())[0]
                    if file in res[word][doc_id].keys():
                        word_list = res[word][doc_id][file][1]
                        word_list += [word_count]
                        aux = (len(word_list), word_list)
                        res[word][doc_id][file] = aux
                    else:
                        res[word][doc_id][file] = (1,[word_count])
                        aux = res[word][doc_id]
                        res[word] = {doc_id+1:aux}
                else:
                    res[word] = {1:{file:(1,[word_count])}}

        return res
        #print(json.dumps(res, indent=4))
        
    def get_documents(self, words):
        result = []
        count = 0
        keys = self.complete_index.keys()
        new_words = []
        for word in words:
            new_words.append(word)
            count += 1
            if word in keys:
                for i in self.complete_index[word].keys():
                    aux = self.complete_index[word][i]
                    #print(word, ' --- ', aux)
                    #print(list(aux.items()))
                    result.append(list(aux.items()))
        #for i in result:
            #print(i)
        #print(colored(new_words, 'yellow'))
        return result, count, new_words

    def same_doc_id(self, documents):
        result = True
        aux = documents[0].copy()
        min_doc_id = aux[0][0]
        for document in documents[1:]:
            if document[0][0] < min_doc_id:
                min_doc_id = document[0][0]
            if aux[0][0] != document[0][0]:
                result = False
        return result, min_doc_id

    def consecutive(self, documents, length, words):
        result = {}
        for i in range(0, len(words)):
            #print('\t\t\t', documents[i][0][1][1])
            decode_bits = getattr(Compresion, 'decode_' + self.compresion)(documents[i][0][1][1])
            #print('DECODE_BITS: ', decode_bits)
            index = decode_bits[0]
            result[index] = words[i]

            for occurence in decode_bits[1:]:
                #print(occurence)
                index += occurence
                result[index] = words[i]

        sorted_x = sorted(result.items(), key=operator.itemgetter(0))
        #print(sorted_x)
        line = ' '.join(words)
        count = 1
        
        head = sorted_x[0]
        aux_line = [sorted_x[0][1]]
        for elem in sorted_x[1:]:
            if head[0] + 1 == elem[0]:
                aux_line.append(elem[1])
                #print(head, elem)
                count += 1
                if count == length and line == ' '.join(aux_line):
                    #print(colored('match', 'green'))
                    return True, documents[0][0][0]
            else: 
                count = 1
            head = elem
        return False, -1

        '''print('\n\n\nCONSECUTIVE???')
        aux = documents[0][0][1][1][0]
        index = 1
        for document in documents[1:]:
            print(aux, document[0][1][1][0])
            if aux + 1 == document[0][1][1][0]:
                index += 1
                if index == length:
                    print(colored('match', 'green'))
                    return True, document[0][0]
                aux = document[0][1][1][0]
            else:
                for i in range(0, index):
                    print(documents, i)
                    documents[i][0][1][1].pop(0)
                    count = documents[i][0][1][0]
                    if documents[i][0][1][1] == []:
                        return False, -1
                    print(documents, i)
                return self.consecutive(documents, length, words)
        return False, -1

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
        return False, -1'''

    def advance_min(self, documents, min_doc_id):
        for document in documents:
            #print(document)
            if document[0][0] == min_doc_id:
                document.pop(0)
                if document == []:
                    return False
        return True

    # precondicion: todas las listas tienen al menos un elemento, sino salimos
    # por construccion sabemos que si la frase esta en las listas n1 n1, nn, podemos
    # asegurar que las palabras están en el mismo orden
    def intersect(self, documents, length, words):
        cont = True
        answer = {}
        while cont:
            result, min_doc_id = self.same_doc_id(documents)
            if result:
                result, file = self.consecutive(documents, length, words)
                #print('ok: ', result, file)
                if result:
                    answer[file] = self.files[file]
            cont = self.advance_min(documents, min_doc_id)
        return answer

    def query_one_word(self, documents):
        result = {}
        for document_list in documents:
            for document in document_list:
                #print(document[0])
                result[document[0]] = self.files[document[0]]
        return result

    def consulta_frase(self, frase):
        words = extrae_palabras(frase)
        documents, count, new_words = self.get_documents(words)
        if len(documents) == 1:
            result = self.query_one_word(documents)
            #print(json.dumps(result, indent=4, sort_keys=True))
        else:
            result = self.intersect(documents, count, new_words)
            #print(json.dumps(result, indent=4, sort_keys=True))
        return result

    def num_bits(self):
        pass


if __name__ == '__main__':
    call(['clear'])
    getattr(Compresion, 'show_out')()
    print('end')
    #getattr(CallMe, variable)()
    vectorialIndex = CompleteIndex(sys.argv[1], 'unary')
    result = vectorialIndex.consulta_frase('rarely ever answer them')
    print(result)













'''
El nuevo parámetro compresion contendrá uno de los siguientes valores:
1. None : No aplicar ninguna compresión, es decir, genera el ı́ndice igual que el apartado A.
2. ’unary’ : Codificación en formato unario.
3. ’variable−bytes’ : Codificación en formato de bytes variables.
4. ’elias−gamma’ : Codificación en formato de Elias-γ.
5. ’elias−delta’ : Codificación en formato de Elias-δ.
Cuando se utilice compresión el ı́ndice invertido completo no almacenará las diferencias de po-
siciones en una lista Python sino que codificará la secuencia como una lista de bits usando un
objeto bitarray .
'''
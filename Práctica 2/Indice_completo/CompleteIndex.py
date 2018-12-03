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
    """
    Devuelve un diccionario con todos los ficheros en el directorio path.

    Usa os.walk para obtener los nombre de todos los ficheros. Para conseguir
    la ruta completa concatenamos el path que le indicamos a la ruta relativa
    hasta el fichero además del nombre del mismo. El diccionario tiene la forma:

        {0 : 'path1'}
        {1 : 'path2'}

    Esto es así para minimizar el tamaño del indice al usar un 'alias' para cada ruta.

    Parametros
    ----------
    path : str
        Ruta de la que leer los ficheros 

    Retorno
    -------
    dict
        Diccionario con los ficheros

    """

    result = {}
    files_count = 0
    for root, dirs, files in os.walk(path):
        for f in files:
            result[files_count] = path + os_path.relpath(os_path.join(root, f), path) 
            files_count += 1
    return result



class CompleteIndex(object):

    def __init__(self, path, compresion = 'none'):
        self.compresion = compresion
        self.files = get_files_dict(path)
        self.bits, self.complete_index = self.create_complete_index()

    def create_complete_index(self):
        """
        Crea el indice completo.
        
        Obtiene todos los ficheros del path indicado y crea el indice invertido completo.
        A continuacion transfoam las listas de apariciones en listas de diferencias y 
        aplica la codificacion indicada si se paso como parametro

        Retorno
        -------
        int
            Bits usados para la codificacion
        dict
            Indice invertido completo

        """
        result = {}
        bits = 0
        for file in self.files.items():
            with open(file[1], 'r', encoding='latin1') as input_file:
                self.get_words(input_file, result, file[0])
        bits, result = getattr(Compresion, 'apply_default')(result)

        if self.compresion != 'none':
            bits = 0
            for res in result.items():
                doc_card = list(res[1].keys())[0]
                for elem in res[1][doc_card].items():
                    new_codec = getattr(Compresion, 'code_' + self.compresion)(elem[1][1])
                    bits += new_codec[0]
                    res[1][doc_card][elem[0]] = new_codec
        return bits, result

    def get_words(self, input_file, res, file):
        """
        Indexa las palabras de un fichero en el indice.

        Parametros
        ----------
        input_file : str
            Stream de entrada de datos 
        res : dict
            Indice invertido a completar 
        file : str
            'alias' del fichero 

        Retorno
        -------
        dict
            Indice invertido parcial

        """

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
        
    def get_documents(self, words):
        """
        Devuelve una lista de ficheros asociada a palabras concretas.

        Parametros
        ----------
        words : list
            Lista de palabras 

        Retorno
        -------
        dict
            Indice invertido parcial
        int
            Numero de palabras en la consulta
        list
            Palabras de la consulta
        """

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
                    result.append(list(aux.items()))
        

        return result, count, new_words

    def same_doc_id(self, documents):
        """
        Comprueba si todos las listas de documentos tinen la misma el mismo head.

        Recorre todas las listas para ver el identificador minimo a eliminar.

        Parametros
        ----------
        documents : list(list)
            Lista de lista de documentos 

        Retorno
        -------
        bool
            Todas las cabezas de lista son iguales
        int
            Cabeza de lista con menor identificador
        """

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
        """
        Comprueba si hay palabras consecutivas en algun fichero.

        Descomprime las listas de apariciones y crea un diccionario con todas las palabras del mismo fichero.
        El diccionario tiene la forma:

                {posicion_i : palabra_j}

        A continuacion ordenamos el diccionario y lo convertimos a lista para buscar la consulta dentro de la misma.

        Parametros
        ----------
        documents : list
            Conjunto de documentos y apariciones 
        length : int
            Tamaño de la consulta 
        words : list
            Conjunto de terminos de la consulta 

        Retorno
        -------
        bool
            True si se ha encontrado la consulta, false en otro caso
        int
            Indice del fichero, en otro caso -1
        """

        result = {}
        for i in range(0, len(words)):
            decode_bits = documents[i][0][1][1]
            if self.compresion != 'none':
                decode_bits = getattr(Compresion, 'decode_' + self.compresion)(documents[i][0][1][1])

            index = decode_bits[0]
            result[index] = words[i]

            for occurence in decode_bits[1:]:
                index += occurence
                result[index] = words[i]

        sorted_x = sorted(result.items(), key=operator.itemgetter(0))
        line = ' '.join(words)
        count = 1
        
        head = sorted_x[0]
        aux_line = [sorted_x[0][1]]
        for elem in sorted_x[1:]:
            if head[0] + 1 == elem[0]:
                aux_line.append(elem[1])
                count += 1
                if count == length and line == ' '.join(aux_line):
                    return True, documents[0][0][0]
            else: 
                count = 1
            head = elem
        return False, -1

    def advance_min(self, documents, min_doc_id):
        """
        Elimina de cada lista de documentos la cabeza con el menor identificador.

        Parametros
        ----------
        documents : list
            Conjunto de listas de documentos 
        min_doc_id : int
            Identificador menor del conjunto de documentos  

        Retorno
        -------
        bool
            True si no hay listas vacias, False en otro caso

        """

        for document in documents:
            if document[0][0] == min_doc_id:
                document.pop(0)
                if document == []:
                    return False
        return True

    def intersect(self, documents, length, words):
        """
        Interseca el conjunto de listas de apariciones de palabras.

        Parametros
        ----------
        documents : list
            Conjunto de listas de documentos 
        length : dict
            Tamaño de la lista de palabras 
        words : list
            Lista de palabras 

        Retorno
        -------
        dict
            Conjunto de ficheros en los que se ha encontrado la consulta

        """

        cont = True
        answer = {}
        if documents == []:
            return answer
        while cont:
            result, min_doc_id = self.same_doc_id(documents)
            if result:
                result, file = self.consecutive(documents, length, words)
                if result:
                    answer[file] = self.files[file]
            cont = self.advance_min(documents, min_doc_id)
        return answer

    def query_one_word(self, documents):
        """
        Ejecuta una query con solo un termino.

        Esta funcion abrevia el calculo con consultas de un termino, solo devuelve la lista de documentos asociados
        a una palabra en el indice invertido

        Parametros
        ----------
        documents : list
            Conjunto de documentos

        Retorno
        -------
        dict
            Ficheros con la consulta

        """

        result = {}
        for document_list in documents:
            for document in document_list:
                result[document[0]] = self.files[document[0]]
        return result

    def consulta_frase(self, frase):
        """
        Genera la respuesta a una consulta del usuario.

        Parametros
        ----------
        frase : str
            Consulta del usuario 

        Retorno
        -------
        dict
            Conjunto de ficheros en los que aparece la consulta

        """

        words = extrae_palabras(frase)
        documents, count, new_words = self.get_documents(words)
        if len(documents) == 1:
            result = self.query_one_word(documents)
        else:
            result = self.intersect(documents, count, new_words)
        return result

    def num_bits(self):
        """
        Devuelve el numero de bits usados para codificar el indice.

        Retorno
        -------
        int
            Bits usados

        """

        return self.bits


if __name__ == '__main__':
    # Ejemplo de uso
    vectorialIndex = CompleteIndex(sys.argv[1], sys.argv[2])
    result = vectorialIndex.consulta_frase('religion foundation')
    if result == {}:
        print(colored('Error', 'red'))
    else:
        for res in result.items():
            print(colored(res[1], 'green'))

    num_bits = vectorialIndex.num_bits()
    print(colored('Bits usados en el indice: ' + str(num_bits), 'blue'))

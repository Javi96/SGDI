'''Antonio Barral Gago y Enrique Campos Martínez declaramos que esta solución es fruto exclusivamente
de nuestro trabajo personal. No hemos sido ayudados por ninguna otra persona ni hemos
obtenido la solución de fuentes externas, y tampoco hemos compartido nuestra solución
con nadie. Declaramos además que no hemos realizado de manera deshonesta ninguna otra
actividad que pueda mejorar nuestros resultados ni perjudicar los resultados de los demás. '''

import string
import os
from glob import iglob
import numpy
from bitarray import bitarray


# Dada una linea de texto, devuelve una lista de palabras no vacias 
# convirtiendo a minusculas y eliminando signos de puntuacion por los extremos
# Ejemplo:
#   > extrae_palabras("Hi! What is your name? John.")
#   ['hi', 'what', 'is', 'your', 'name', 'john']
def extrae_palabras(linea):
	return filter(lambda x: len(x) > 0, map(lambda x: x.lower().strip(string.punctuation), linea.split()))


#Comprueba si la cabecera de todas las palabras pertenecen al mismo docID
def sameDoc(word_dict):
	docID = 0

	for word, doc_list in word_dict.items():
		if(docID == 0):
			docID = doc_list[0][0]

		else:
			if docID != doc_list[0][0]:
				return False

	return True


#Comprueba si las apariciones son consecutivas. Es decir, forman una frase
def consecutive(word_dict):
	possible_first_pos = []
	possible_next_pos = []
	current_pos = []

	for word, doc_list in word_dict.items():
		if(possible_first_pos == []):
			possible_first_pos = doc_list[0][1]
			possible_next_pos = [x+1 for x in possible_first_pos]

		else:
			current_pos = doc_list[0][1]
			possible_next_pos = [n for n in current_pos if n in possible_next_pos] #Interseccion para comprobar cuales son consecutivos

			if possible_next_pos == []:
				return False

			possible_next_pos = [x+1 for x in possible_next_pos] #Suma 1 a cada elemento para comprobar en la siguiente iteracion
			


	return True


#Elimina las cabeceras de las palabras con menor docID
def advanceMin(word_dict):
	min_doc = 0
	min_words = []

	#Primero almacena las palabras con menor docID
	for word, doc_list in word_dict.items():
		if min_doc == 0:
			min_doc = doc_list[0][0]
			min_words.append(word)

		else:
			if min_doc > doc_list[0][0]:
				min_doc = doc_list[0][0]
				min_words = [word]

			elif min_doc == doc_list[0][0]:
				min_words.append(word)

	#Despues remueve la cabecera de dichos documentos
	for word, doc_list in word_dict.items():
		if word in min_words:
			doc_list.pop(0)

	return word_dict


#Devuelve falso en caso de que ya se haya recorrido todas las apariciones de un documento 
def noNil(word_dict):
	for word, doc_list in word_dict.items():
		if doc_list == []:
			return False

	return True


#Calcula que documentos tienen la frase
def intersect_phase(word_dict, file_dict):
	contin = True

	answer = []

	while contin:
		if sameDoc(word_dict):
			if consecutive(word_dict):
				index = word_dict[list(word_dict.keys())[0]][0][0] #First 0 -> first word. Second 0 -> first value in the list. Third 0 -> file index in tuple
				answer.append(file_dict[index])
			word_dict = advanceMin(word_dict)

		else:
			word_dict = advanceMin(word_dict)

		contin = noNil(word_dict)

	return answer


#Calcula cada posicion teniendo en cuenta la diferencia con el anterior
def pos_diff(numbers):
	diff_list = list(numpy.diff(numbers))
	diff_list.insert(0, numbers[0])
	return diff_list


#Realiza compresion unaria
def unary_compresion(numbers):
	new_list = []

	for number in numbers:
		b = bitarray()

		for i in range(number-1):
			b.append(True)

		b.append(False)
		new_list.append(b)

	return new_list


#Realiza compresion por bytes variables
def variable_b_compresion(numbers):
	new_list = []

	for number in numbers:
		b = bitarray()

		if number < 128:
			binary = format(number, "07b")
			b.append(1)
			b.extend(binary)

		else:
			length = len(format(number, "b"))
			binary = format(number, "0" + str(length + (length % 7)) + "b") #Se le añaden tantos 0's como sean necesarios hasta llegar a multiplo de 7
			
			first_iter = True

			for i in range(int(len(binary)/7)):
				b.append(first_iter)
				b.extend(binary[i*7:(i+1)*7])

				if(first_iter):
					first_iter = False


		new_list.append(b)

	return new_list


#Realiza compresion elias-gamma
def elias_gamma_compresion(numbers):
	new_list = []

	for number in numbers:
		b = bitarray()

		if number == 1:
			b.append(0)

		else:
			binary = format(number, "b")
			offset = binary[1:] #Elimina el primer 1
			length = unary_compresion([len(binary)]) #Calcula el unario de la longitud del binario

			b.extend(length)
			b.extend(offset)

		new_list.append(b)

	return new_list


#Realiza compresion elias-delta
def elias_delta_compresion(numbers):
	new_list = []

	for number in numbers:
		b = bitarray()

		if number == 1:
			b.append(0)

		else:
			binary = format(number, "b")
			offset = binary[1:] #Elimina el primer 1
			length = elias_gamma_compresion([len(binary)]) #Calcula el elias-gamma de la longitud del binario

			b.extend(length)
			b.extend(offset)

		new_list.append(b)

	return new_list



class CompleteIndex(object):


	def __init__(self, directorio, compresion=None):
		self.document_counter = 1
		self.inverted_index = {}
		self.file_dict = {}
		self.compresion = compresion
		self.bits_number = 0

		root = directorio + '/**/*'
		file_list = [f for f in iglob(root, recursive=True) if os.path.isfile(f)]
		file_counter = 1
		file_dict = {}
		for file in file_list:
			self.process_document(file, file_counter)
			file_counter += 1

		return


	#Estructura del indice: {'palabra': [(documento, [pos1, pos2, pos3, ..., posn]), (tupla2), (tupla3)]}
	def process_document(self, file, file_counter):
		words_used = []

		with open(file, 'r',encoding="latin1") as f:
			lines = f.readlines()
			self.file_dict[file_counter] = f.name

			word_count = 1
			for line in lines:
				line_cleaned = extrae_palabras(line)
				
				for word in line_cleaned:

					#Si la palabra no esta en el indice
					if word not in self.inverted_index:
						self.inverted_index[word] = [(file_counter, [word_count])]

					#Si la palabra esta en el indice pero el fichero actual todavia no ha incluido ninguna palabra
					elif self.inverted_index[word][-1][0] != file_counter:
						self.inverted_index[word].append((file_counter, [word_count]))

					#La palabra esta en el indice y ya ha habido ocurrencias del fichero actual
					else:
						self.inverted_index[word][-1][-1].append(word_count)

					word_count += 1

					if word not in words_used:
						words_used.append(word)


					#En caso de que no haya compresion, el numero de bits por cada palabra es 32
					if self.compresion == None:
						self.bits_number += 32


		#En caso de que se quiera realizar una compresion, se le pasan las palabras modificadas del indice y se aplica
		if self.compresion != None:
			self.compresion_funct(words_used)

		return


	#Dependiendo del metodo de compresion se llama a un algoritmo o a otro
	def compresion_funct(self, words_used):
		for word in words_used:
			new_appearances_list = []
			current_tuple = self.inverted_index[word][-1]
			appearances_list = pos_diff(current_tuple[-1])

			if self.compresion == 'variable−bytes':
				new_appearances_list = variable_b_compresion(appearances_list)

			elif self.compresion == 'unary':
				new_appearances_list = unary_compresion(appearances_list)

			elif self.compresion == 'elias-gamma':
				new_appearances_list = elias_gamma_compresion(appearances_list)

			else:
				new_appearances_list = elias_delta_compresion(appearances_list)
						
			self.inverted_index[word][-1] = (current_tuple[0], new_appearances_list)

			for appearance in new_appearances_list:
				self.bits_number += len(appearance)



	#Realiza una consulta de una frase sobre el indice invertido completo
	def consulta_frase(self, frase):
		word_dict = {}
		frase = frase.split()
		for word in frase:
			word_dict[word] = self.inverted_index[word]

		answer = intersect_phase(word_dict, self.file_dict)

		print(answer)


	def num_bits(self):
		return self.bits_number


i = CompleteIndex('20news_18828', 'unary')
#i.consulta_frase('character to')
print(i.num_bits())

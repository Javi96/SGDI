"""
Aitor Cayón Ruano y José Javier Cortés Tejada declaramos que esta solución es fruto exclusivamente
de nuestro trabajo personal. No hemos sido ayudados por ninguna otra persona ni hemos
obtenido la solución de fuentes externas, y tampoco hemos compartido nuestra solución
con nadie. Declaramos además que no hemos realizado de manera deshonesta ninguna otra
actividad que pueda mejorar nuestros resultados ni perjudicar los resultados de los demás.
"""

import requests
import xml.etree.ElementTree as ET

def librosInformatica() :
	# Consulta XQuery que queremos ejecutar
	q = """	for $e in doc('/db/sgdi/books.xml')/catalog/book
			let $t := $e/title
			where $e/genre = 'Computer'
			order by $e/title
			return $t """

	# Creamos un diccionario con los datos a enviar en la peticion
	req = {'_query': q}

	# Realizamos la peticion al servidor REST de eXist−db
	r = requests.post('http://localhost:8080/exist/rest/db', data=req)

	# Creamos un arbol XML a partir del texto de la respuesta ’r’
	root = ET.fromstring(r.text)

	# Creamos una lista con los resultados
	lista = []
	for child in root:
		lista.append(child.text)

	return lista

def libroBK105() :
	# Consulta XQuery que queremos ejecutar
	q = """	for $e in doc('/db/sgdi/books.xml')/catalog/book
			let $t := $e/title
			where $e/@id = 'bk105'
			return $t """

	# Creamos un diccionario con los datos a enviar en la peticion
	req = {'_query': q}

	# Realizamos la peticion al servidor REST de eXist−db
	r = requests.post('http://localhost:8080/exist/rest/db', data=req)

	# Creamos un arbol XML a partir del texto de la respuesta ’r’
	root = ET.fromstring(r.text)

	lista = []
	for child in root:
		lista.append(child.text)

	return lista

def precioLibros() :
	# Consulta XQuery que queremos ejecutar
	q = """	for $e in doc('/db/sgdi/books.xml')/catalog/book
			let $t := $e/title, $p := $e/price
			return ($t,$p) """

	# Creamos un diccionario con los datos a enviar en la peticion
	req = {'_query': q}

	# Realizamos la peticion al servidor REST de eXist−db
	r = requests.post('http://localhost:8080/exist/rest/db', data=req)

	# Creamos un arbol XML a partir del texto de la respuesta ’r’
	root = ET.fromstring(r.text)

	lista = []

	for child in root:
		print(child.text)
		#lista.append((child.title.text, child.price.text))

	return lista


print("Libros informática: ", librosInformatica(), "\n")
print("Libro BK105: ", libroBK105(), "\n")
print("Precios: ", precioLibros(), "\n")
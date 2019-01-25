"""
Aitor Cayón Ruano y José Javier Cortés Tejada declaramos que esta solución es fruto exclusivamente
de nuestro trabajo personal. No hemos sido ayudados por ninguna otra persona ni hemos
obtenido la solución de fuentes externas, y tampoco hemos compartido nuestra solución
con nadie. Declaramos además que no hemos realizado de manera deshonesta ninguna otra
actividad que pueda mejorar nuestros resultados ni perjudicar los resultados de los demás.
"""

import requests
import xml.etree.ElementTree as ET
import json as json

def request(query):
	"""
    Ejecuta una query en el servidor REST de eXist-db

    Parametros
    ----------
    query : str
        Consulta a ejecutar 

    Retorno
    -------
    ET.fromstring(response.text)
        Element

    """

    # Realizamos la peticion al servidor REST de eXist−db
    response = requests.post('http://localhost:8080/exist/rest/db', data={'_query': query})

    # Creamos un arbol XML a partir del texto de la respuesta ’r’
    return ET.fromstring(response.text)


def query1() :
    """
    Obtiene el título de todos los libros de informática.

    Retorno
    -------
    lista
        list

    """

    query = """ 
            for $e in doc('/db/sgdi/books.xml')/catalog/book
            let $t := $e/title
            where $e/genre = 'Computer'
            order by $e/title
            return $t 
            """

    response = request(query)

    # Creamos una lista con los resultados
    lista = []
    for child in response:
        lista.append(child.text)

    return lista

def query2() :
	"""
    Obtiene el título del libro con identificador “bk105”.

    Retorno
    -------
    lista
        list

    """

    query = """    
    		for $e in doc('/db/sgdi/books.xml')/catalog/book
            let $t := $e/title
            where $e/@id = 'bk105'
            return $t """

    response = request(query)

    lista = []

    for child in response:
        lista.append(child.text)

    return lista

def query3() :
	"""
    Obtiene tuplas <titulo, precio> de todos los libros.

    Retorno
    -------
    lista
        list

    """

    query = """ 
    		for $e in doc('/db/sgdi/books.xml')/catalog/book
            let $t := $e/title, $p := $e/price
            return ($t,$p) """

    response = request(query)

    lista = []

    i = 0

	# Creamos una lista con los resultados. Tomamos los elementos de dos en dos para generar las tuplas.
    while i != len(response):
        lista.append((response[i].text, response[i+1].text))
        i+=2

    return lista

def query4() :
	"""
    Obtiene los nombres de los libros que cuestan mas de 15 euros ordenados por fecha de publicación ascendente.

    Retorno
    -------
    lista
        list

    """

    query = """ 
    		for $e in doc('/db/sgdi/books.xml')/catalog/book
            where $e/price > 15
            order by $e/publish_date ascending
            return $e/title """

    response = request(query)

    lista = []

    for i in response:
        lista.append(i.text)

    return lista

def query5() :
	"""
    Obtiene tuplas <titulo, autor, precio> de los libros de género "Computer" que cuestan más de 40 euros.

    Retorno
    -------
    lista
        list

    """

    query = """ 
    		for $e in doc('/db/sgdi/books.xml')/catalog/book
            where $e/price > 40 and $e/genre = 'Computer'
            return ($e/title, $e/author, $e/price) """

    response = request(query)

    lista = []

    i = 0

	# Creamos una lista con los resultados. Tomamos los elementos de tres en tres para generar las tripletas.
    while i != len(response):
        lista.append((response[i].text, response[i+1].text, response[i+2].text))
        i+=3

    return lista

if __name__ == '__main__':
    print('\n\n')
    [print(x) for x in query1()]
    print('\n\n')
    [print(x) for x in query2()]
    print('\n\n')
    [print(x) for x in query3()]
    print('\n\n')
    [print(x) for x in query4()]
    print('\n\n')
    [print(x) for x in query5()]

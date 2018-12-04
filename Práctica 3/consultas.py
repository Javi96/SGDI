'''JoseJavierCortesTejada y AitorCayonRuano declaramos que esta solución
es fruto exclusivamente de nuestro trabajo personal. No hemos sido
ayudados por ninguna otra persona ni hemos obtenido la solución de
fuentes externas, y tampoco hemos compartido nuestra solución con
nadie. Declaramos además que no hemos realizado de manera desho-
nesta ninguna otra actividad que pueda mejorar nuestros resultados
ni perjudicar los resultados de los demás.'''

# -*- coding: utf-8 -*-
import pymongo
from pymongo import MongoClient
from termcolor import colored
from termcolor import *
from subprocess import call
from bson import ObjectId

# 1. Fecha y título de las primeras 'n' peliculas vistas por el usuario 'user_id'
# usuario_peliculas( 'fernandonoguera', 3 )
def usuario_peliculas(user_id = 'fernandonoguera', n = 3):
    print(colored('usuario_peliculas', 'yellow'))
    return usuarios.find({'_id':user_id}, {'_id': 0, 'visualizaciones': { '$slice': n }, 'visualizaciones.fecha': 1, 'visualizaciones.titulo': 1})
           
# 2. _id, nombre y apellidos de los primeros 'n' usuarios a los que les gusten 
# varios tipos de película 'gustos' a la vez
# usuarios_gustos(  ['terror', 'comedia'], 5  )
def usuarios_gustos(gustos = ['terror', 'comedia'], n = 5):
    print(colored('usuarios_gustos', 'yellow'))
    return usuarios.find({'gustos': {'$all': gustos}}, {'nombre':1, 'apellido1':1, 'apellido2':1}).limit(n)
  
# 3. _id de usuario de sexo 'sexo' y con una edad entre 'edad_min' e 'edad_max' incluidas
# usuario_sexo_edad('M', 50, 80)
def usuario_sexo_edad( sexo = 'M', edad_min = 50, edad_max = 80):
    print(colored('usuario_sexo_edad', 'yellow'))
    return usuarios.find({'sexo': sexo, 'edad':{'$gte': edad_min, '$lte':edad_max}},{'_id':1})

# 4. Nombre, apellido1 y apellido2 de los usuarios cuyos apellidos coinciden,
#    ordenado por edad ascendente
# usuarios_apellidos()
def usuarios_apellidos():
    print(colored('usuarios_apellidos', 'yellow'))
    return usuarios.find(
                            {'$expr': { '$eq': [ "$apellido1" , "$apellido2" ]}}, 
                            {'_id': 0, 'nombre': 1, 'apellido1': 1, 'apellido2': 1}).sort('edad', pymongo.ASCENDING)
    
# 5.- Titulo de peliculas cuyo director empiece con un 'prefijo' dado
# pelicula_prefijo( 'Yol' )
def pelicula_prefijo( prefijo = 'Yol' ):
    print(colored('pelicula_prefijo', 'yellow'))
    return peliculas.find({'director': {'$regex': '^' + prefijo, '$options': 'i'}}, {'_id': 0, 'titulo': 1})
        
# 6.- _id de usuarios con exactamente 'n' gustos, ordenados por edad descendente
# usuarios_gustos_numero( 6 )
def usuarios_gustos_numero(n = 6):
    print(colored('usuarios_gustos_numero', 'yellow'))
    return usuarios.find({'gustos': {'$size': n}}, {'_id': 1}).sort('edad', pymongo.DESCENDING)  
    
# 7.- usuarios que vieron pelicula la pelicula 'id_pelicula' en un periodo 
#     concreto 'inicio' - 'fin'
# usuarios_vieron_pelicula( '583ef650323e9572e2812680', '2015-01-01', '2016-12-31' )
def usuarios_vieron_pelicula(id_pelicula = '583ef650323e9572e2812680', inicio = '2015-01-01', fin = '2016-12-31'):
    print(colored('usuarios_vieron_pelicula', 'yellow'))
    return usuarios.find(
                            {'visualizaciones._id': ObjectId(id_pelicula), 'visualizaciones.fecha': {"$gte": inicio}, 'visualizaciones.fecha': {"$lte": fin}}, 
                            {'_id': 1})


if __name__ == '__main__':
    client = MongoClient('localhost', 27017)
    db = client['sgdi_pr3']
    usuarios = db['usuarios']
    peliculas = db['peliculas']
    call(['clear'])
    
    [print(colored(x, 'green')) for x in usuario_peliculas()]
    [print(colored(x, 'green')) for x in usuarios_gustos()]
    [print(colored(x, 'green')) for x in usuario_sexo_edad()]
    [print(colored(x, 'green')) for x in usuarios_apellidos()]
    [print(colored(x, 'green')) for x in pelicula_prefijo()]
    [print(colored(x, 'green')) for x in usuarios_gustos_numero()]
    [print(colored(x, 'green')) for x in usuarios_vieron_pelicula()]

'''

use sgdi_pr3
db.createCollection('peliculas')

Los import en la cmd
    mongoimport --db sgdi_pr3 --collection peliculas --file peliculas.json
    mongoimport --db sgdi_pr3 --collection usuarios --file usuarios.json

'''


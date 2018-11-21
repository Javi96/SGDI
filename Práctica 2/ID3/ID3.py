# Insertar aqui la cabecera
import json
import csv
import sys
import operator
import random
import math
from termcolor import colored
from termcolor import *
from subprocess import call

class Node():

    node_count = 0

    def __init__(self, _type, value = '', edge = ''):
        self.value = value
        self.edge = edge
        self.type = _type
        self.nodes = []
        Node.node_count += 1
        self.node_id = Node.node_count 
        self.node_info = ''

    def add_son(self, son):
        """
        Añade un nuevo nodo a la lista de hijos.

        Parametros
        ----------
        son : Node
            Hijo a añadir a la lista

        """

        self.nodes.append(son)

    def get_tree(self):
        """
        Genera una cadena de texto con la información del nodo.

        Recopila la información de un nodo y sus hijos para generar un fichero .dot.
        Para ello toma de cada nodo su identificador y le añade una etiqueta 'label'
        con su valor. Si el nodo en cuestión no tiene hijos añade una nueva etiqueta
        'shape' para modificar la representación del mismo en el .dot. También toma
        las aristas que conectan los nodos entre sí.

        Retorno
        -------
        str
            Información del nodo

        """

        if len(self.nodes) == 0:
            self.node_info += str(self.node_id) + '''[label="''' + str(self.value) + '''", shape="box"];\n'''
        else:
            self.node_info += str(self.node_id) + '''[label="''' + str(self.value) + '''"];\n'''
        for node in self.nodes:
            self.node_info += str(self.node_id) + '->' + str(node.node_id) + '''[label="''' + node.edge + '''"];\n'''
            self.node_info += node.get_tree()
        return self.node_info


class ID3Tree():

    def __init__(self, data, attributes):
        self.attributes = attributes
        self.data = data
        self.nodo = self.generate_tree(attributes, data)

    def get_major_class(self, instances):
        """
        Devuelve la clase que más aparece en el conjunto de instancias.

        Genera un diccionario para cada valor del atributo 'class' del
        conjunto de instancias. Hecho esto devuelve la clase que más se
        repite y el numero de clases distintas de la entrada.

        Parametros
        ----------
        instances : list
            Conjunto de instancias a evaluar

        Retorno
        -------
        str
            Clase que más se repite 
        int
            Numero de clases distintas
        """

        classes = {}
        for instance in instances:
            class_value = instance.get('class')
            if class_value in classes:
                classes[class_value] = 1 + classes[class_value]
            else:
                classes[class_value] = 1
        major_class = max(classes.items(), key=operator.itemgetter(1))[0]
        return major_class, len(classes)

    def group_by_attribute(self, attributes, instances):
        """
        Agrupa el conjunto de instancias en función del valor de cada atributo.

        Genera un diccionario con la forma:
            {attr1 : 
                {valor1 : [instA, ..., instN]}
                {valor2 : [instB, ..., instM]}
            {attr2 : 
                {valor2 : ...}
        donde atributo almacena una lista de instancias en funcion de los valores
        del mismo.

        Parametros
        ----------
        attributes : list
            Lista de atributos 
        instances : list
            Conjunto de instancias a clasificar

        Retorno
        -------
        dict
            Diccionario con las instancias clasificadas

        """

        result = {}
        for attribute in attributes: # para cada atributo
            if attribute == 'class':
                continue
            grouped_instances = {}
            for instance in instances:
                copy_instance = instance.copy()
                copy_instance.pop(attribute)
                if instance[attribute] in grouped_instances:
                    grouped_instances[instance[attribute]] = [copy_instance] + grouped_instances[instance[attribute]]
                else:
                    grouped_instances[instance[attribute]] = [copy_instance]
            result[attribute] = grouped_instances
        return result

    def get_partition(self, instances, attribute, value):
        """
        Genera un nuevo conjunto de instancias en base a un valor concreto de un atributo conocido.

        Parametros
        ----------
        instances : list
            Lista de atributos 
        attribute : str
            Atributo concreto a usar
        value : str
            Valor del atributo por el que filtrar

        Retorno
        -------
        list
            Lista de instancias filtradas

        """

        partition = []
        for instance in instances:
            if instance[attribute] == value:
                partition.append(instance)
        return partition

    def generate_tree(self, attributes, instances, edge = ''):
        """
        Genera el arbol de clasificacion.

        Completa un nodo que representa el arbol de clasificacion. Para ello:

            -   agrupa las instancias por el valor de la clase. Si no quedan 
                atributos por los que clasificar o todas las instancias tienen
                la misma clase se termina de expandir la rama actual.

            -   genera un conjunto de instancias para los valores de cada atributo
                y encuentra el atributo con menor entropia (mayor ganancia de informacion).

            -   genera particiones del conjunto de instancias inicial y expande el arbol 
                usando cada posible valor del atributo con menor entropia.

        Parametros
        ----------
        attributes : list
            Lista de atributos 
        instances : list
            Conjunto de instancias
        edge : str
            Valor del atributo usado en la anterior expansion del arbol

        Retorno
        -------
        Node
            Nodo con el arbol de clasificación

        """

        
        major_class, count_class = self.get_major_class(instances)
        if count_class == 1 or len(attributes) == 1:
            return Node('leaf', major_class, edge)
        else:
            group_by_attribute = self.group_by_attribute(attributes, instances)
            min_entropy = self.get_entropy(group_by_attribute)
            nodo = Node('inner', min_entropy, edge)
            for elem in group_by_attribute[min_entropy].items():
                new_data = self.get_partition(instances, min_entropy, elem[0])
                if len(new_data) == 0:
                    aux_node = Node('leaf', major_class, edge)
                else:
                    new_attributes = attributes.copy()
                    new_attributes.remove(min_entropy)                  
                    aux_node = self.generate_tree(new_attributes, new_data, elem[0])
                nodo.add_son(aux_node)
        return nodo

    def get_entropy(self, grouped_instances):
        """
        Determina el atributo con menor entropía por el que expandir el arbol.

        Parametros
        ----------
        grouped_instances : dict
            Instancias agrupadas por atributo en base a cada valor 

        Retorno
        -------
        str
            Atributo por el que expandir

        """

        total_entropy = {}
        for group in grouped_instances.items(): # iteramos para cada atributo
            group_by_class = self.group_by_class(group)
            for elem in group_by_class.items(): # para cada valor del atributo
                entropies_and_elems = []
                count = sum(elem[1].values())
                entropy = 0
                for i in elem[1].items(): # para cada valor del atributo calculamos su entropia
                    entropy += -i[1]/count*math.log2(i[1]/count)
                entropies_and_elems.append((entropy, count))
                if group[0] not in total_entropy:
                    total_entropy[group[0]] = entropies_and_elems
                else:
                    total_entropy[group[0]] = entropies_and_elems + total_entropy[group[0]] 
        result = {}
        for attribute in total_entropy.items():
            acc = 0
            for i in range(0, len(attribute[1])):
                acc += attribute[1][i][0]*attribute[1][i][1]/len(self.data)
            result[attribute[0]] = acc
        return min(result.items(), key=operator.itemgetter(1))[0]

    def group_by_class(self, grouped_instances):
        """
        Agrupa las instancias por clase.

        Parametros
        ----------
        grouped_instances : dict
            Instancias agrupadas por atributo en base a cada valor 

        Retorno
        -------
        dict
            Instancias agrupadas por clase

        """

        group_by_class = {}
        for group in grouped_instances[1].items():
            aux = {}
            for instance in group[1]:
                if instance['class'] not in aux:
                    aux[instance['class']] = 1
                else:
                    aux[instance['class']] = 1 + aux[instance['class']]
            group_by_class[group[0]] = aux
        return group_by_class

    def save_tree(self, file):
        """
        Crea una representación del arbol de clasificacion apta para la herramienta xdot.

        Parametros
        ----------
        file : str
            Nombre del fichero sobre el que volcar el arbol 

        """

        with open(file, "w+") as file:
            file.write('''digraph tree {''' + self.nodo.get_tree() + '''}''')

    def clasifica(self, instance, node):
        """
        Clasifica una instancia.

        Parametros
        ----------
        instance : dict
            Instancia a clasificar
        node : Node
            Nodo para expandir el arbol 

        Retorno
        -------
        str
            Valor delvuelto por el clasificador. Puede ser:
                -   el valor del nodo hoja (la clase predicha)
                -   un valor arbitrario para indicar que no se ha clasificado

        """

        if node.type == 'leaf':
            return node.value
        attribute = instance[node.value]
        for child in node.nodes:
            if child.edge == attribute:
                del instance[node.value]
                return self.clasifica(instance, child)
        return '-----'

class ID3(object):

    def __init__(self, file):
        instances, attributes = self.read_csv(file)
        self.tree = ID3Tree(instances, attributes)

    def read_csv(self, file):
        """
        Lee el conjunto de instancias de un fichero .csv.

        Parametros
        ----------
        file : str
            Fichero del que leer los datos

        Retorno
        -------
        list
            Conjunto de instancias
        list
            Lista de atributos
        """

        attributes = []
        instances = []
        with open(file) as input_file:
            line = csv.reader(input_file, delimiter = ',')
            attributes = next(line)
            for word in line:
                instances.append({attributes[i] : word[i] for i in range(0, len(word))})
        return instances, attributes

    def clasifica(self, instance):
        """
        Clasifica una instancia.

        Parametros
        ----------
        instance : dict
            Instancia a clasificar

        Retorno
        -------
        str
            Valor devuelto por el clasificador
        
        """

        return self.tree.clasifica(instance, self.tree.nodo)
        
    def test(self, file):
        """
        Clasifica un conjunto de instancias desde un fichero.

        Parametros
        ----------
        file : str
            Fichero del que leer los datos

        Retorno
        -------
        dict
            Tasa de aciertos, fallos y el numero de instancias clasificadas
        """

        instances, attributes = self.read_csv(file)
        hits = 0
        for instance in instances:
            result = self.tree.clasifica(instance.copy(), self.tree.nodo)
            print('Instance: ', instance, '\t\nresult: ', result)
            if instance['class'] == result:
                hits += 1
                print(colored('Hit', 'green'))
            else:
                print(colored('Fail', 'red'))

        return {'Hits: ': hits, 'Fails: ': len(instances)-hits, 'Total: ': len(instances)}
        

    def save_tree(self, file):
        """
        Guarda la representacion del arbol de clasificacion en un fichero .dot.

        Parametros
        ----------
        file : str
            Fichero en el que escribir los datos
        """

        self.tree.save_tree(file)
        call(['xdot', file])
        
if __name__ == '__main__':
    id3 = ID3(sys.argv[1])
    id3.save_tree('example.dot')
    '''print(colored('class: ' + id3.clasifica({'season':'winter','rain':'heavy','wind':'high','day':'weekday'}), 'yellow'))'''
    #print(colored('class: ' + id3.clasifica({'season':'winter','rain':'heavy','wind':'high','day':'saturday'}), 'yellow'))
    result = id3.test(sys.argv[2])
    print(result)

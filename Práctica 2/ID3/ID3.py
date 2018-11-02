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

    def __init__(self, value = '', edge = ''):
        self.value = value
        self.edge = edge
        self.nodes = []
        Node.node_count += 1
        self.node_id = Node.node_count 
        self.node_info = ''

    def add_son(self, son):
        self.nodes.append(son)

    def show(self, deep = 0):
        acc = ''
        for i in range(0,deep*5):
            acc += ' '
        
        for node in self.nodes:
            #print(acc, node.value, node.edge)
            node.show(deep+1)

    def get_tree(self):
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
        self.len = len(data)
        self.nodo = self.generate_tree(attributes, data)
        self.nodo.show()

    def get_major_class(self, group_by_attribute, instances):
        classes = {}
        for instance in instances:
            class_value = instance.get('class')
            if class_value in classes:
                classes[class_value] = 1 + classes[class_value]
            else:
                classes[class_value] = 1
        #print(colored(json.dumps(classes, indent=4), 'red'))
        major_class = max(classes.items(), key=operator.itemgetter(1))[0]
        return major_class, len(classes)

    def group_by_attribute(self, attributes, data):
        result = {}
        for attribute in attributes: # para cada atributo
            if attribute == 'class':
                continue
            grouped_instances = {}
            for instance in data:
                copy_instance = instance.copy()
                copy_instance.pop(attribute)
                if instance[attribute] in grouped_instances:
                    grouped_instances[instance[attribute]] = [copy_instance] + grouped_instances[instance[attribute]]
                else:
                    grouped_instances[instance[attribute]] = [copy_instance]
            result[attribute] = grouped_instances
        return result

    def get_partition(self, instances, attribute, value):
        partition = []
        for instance in instances:
            if instance[attribute] == value:
                partition.append(instance)
        return partition

    def generate_tree(self, attributes, data, edge = ''):
        group_by_attribute = self.group_by_attribute(attributes, data)
        major_class, count_class = self.get_major_class(group_by_attribute, data)
        if count_class == 1 or len(attributes) == 1:
            return Node(major_class, edge)
        else:
            entropy_by_group = self.get_entropy(group_by_attribute)
            min_entropy = min(entropy_by_group.items(), key=operator.itemgetter(1))[0]
            nodo = Node(min_entropy, edge)
            for elem in group_by_attribute[min_entropy].items():
                new_data = self.get_partition(data, min_entropy, elem[0])
                if len(new_data) == 0:
                    aux_node = Node(major_class, edge)
                else:
                    new_attributes = attributes.copy()
                    new_attributes.remove(min_entropy)                  
                    aux_node = self.generate_tree(new_attributes, new_data, elem[0])
                nodo.add_son(aux_node)
        return nodo



    def get_entropy(self, grouped_instances):
        total_entropy = {}
        for group in grouped_instances.items(): # iteramos para cada atributo
            group_by_class = self.group_by_class(group)
            for elem in group_by_class.items(): # para cada attribute
                entropies_and_elems = []
                count = sum(elem[1].values())
                entropy = 0
                for i in elem[1].items(): # para cada VALOR del ATRIBUTO calculamos su entropia
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
                acc += attribute[1][i][0]*attribute[1][i][1]/self.len
            result[attribute[0]] = acc
        return result

    def group_by_class(self, grouped_instances):
        group_by_class = {}
        for group in grouped_instances[1].items(): # para cada valor del atributo..
            aux = {}
            for instance in group[1]:
                if instance['class'] not in aux:
                    aux[instance['class']] = 1
                else:
                    aux[instance['class']] = 1 + aux[instance['class']]
            group_by_class[group[0]] = aux
        return group_by_class

    def save_tree(self, file):
        with open(file, "w+") as file:
            res = self.nodo.get_tree()
            print(colored(res, 'blue'))
            file.write('''digraph tree {''')
            file.write(res)
            file.write('''}''')

class ID3(object):

    def __init__(self, fichero):
        attributes = []
        data = []
        with open(fichero) as input_file:
            line = csv.reader(input_file, delimiter = ',')
            attributes = next(line)
            for word in line:
                data.append({attributes[i] : word[i] for i in range(0, len(word))})
            print(data)
            print(attributes)
            self.tree = ID3Tree(data, attributes)

    def clasifica(self, instancia):
        pass
        
    def test(self, fichero):
        pass

    def save_tree(self, fichero):
        self.tree.save_tree(fichero)
        call(['xdot', fichero])
        
if __name__ == '__main__':
    id3 = ID3(sys.argv[1])
    id3.save_tree('example.dot')

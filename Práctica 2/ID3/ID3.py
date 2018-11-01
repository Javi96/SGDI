# Insertar aqui la cabecera
import json
import csv
import sys
import operator
import random
import math
from termcolor import colored
from termcolor import *

class Node():

	def __init__(self, value = '', edge = ''):
		self.value = value
		self.edge = edge
		self.nodes = []

	def add_son(self, son):
		self.nodes.append(son)

	def show(self, deep = 0):
			
		acc = ''
		for i in range(0, deep*2):
			acc += '   '

		if len(self.nodes) != 0:
			for i in range(0, 3):
				print('|', acc, '|')
		else: 
			for i in range(0, 3):
				print('|')
		data = ''
		for i in range(0, 7):
			data += '-'
		

		for node in self.nodes:
			print('|', acc, data, node.value, '[', node.edge, ']')
			node.show(deep+1)


class ID3Tree():

	def __init__(self, data, attributes):
		self.attributes = attributes
		self.data = data
		self.len = len(data)
		self.nodo = Node("","")
		self.generate_tree(attributes, data, self.nodo)
		self.nodo.show()

	def get_major_class(self, group_by_attribute, instances):
		classes = {}
		for instance in instances:
			class_value = instance.get('class')
			if class_value in classes:
				classes[class_value] = 1 + classes[class_value]
			else:
				classes[class_value] = 1
		return max(classes.items(), key=operator.itemgetter(1))[0], len(classes)

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

	def generate_tree(self, attributes, data, nodo):
		group_by_attribute = self.group_by_attribute(attributes, data)
		#print(colored(json.dumps(group_by_attribute, indent=4, sort_keys=True), 'green'))
		major_class, count_class = self.get_major_class(group_by_attribute, data)
		if count_class == 1 or len(attributes) == 1:
			nodo.add_son(Node(major_class, ''))
		else:
			entropy_by_group = self.get_entropy(group_by_attribute)
			print(colored(json.dumps(entropy_by_group, indent=4, sort_keys=True), 'green'))

			max_entropy = min(entropy_by_group.items(), key=operator.itemgetter(1))[0]
			print(colored(max_entropy, 'red'))

			for elem in group_by_attribute[max_entropy].items():
				new_data = self.get_partition(data, max_entropy, elem[0])
				new_attributes = attributes.copy()
				new_attributes.remove(max_entropy)
				new_node = Node(max_entropy, elem[0])
				nodo.add_son(new_node)
				self.generate_tree(new_attributes, new_data, new_node)
				
	def get_entropy(self, grouped_instances):
		total_entropy = {}
		for group in grouped_instances.items(): # iteramos para cada atributo
			group_by_class = self.group_by_class(group)
			#print(colored(group[0], 'blue'))
			for elem in group_by_class.items(): # para cada attribute
				#print(elem)
				entropies_and_elems = []
				count = sum(elem[1].values())
				#print(colored(elem[0], 'green'), colored(count, 'green'))
				entropy = 0
				for i in elem[1].items(): # para cada VALOR del ATRIBUTO calculamos su entropia
					#print(colored(i[0], 'yellow'), colored(i[1], 'yellow'))
					entropy += -i[1]/count*math.log2(i[1]/count)
				#print(colored(elem[0], 'yellow'), colored(entropy, 'blue'))
				entropies_and_elems.append((entropy, count))
				#print(colored(json.dumps(elem[1], indent=4, sort_keys=True), 'blue'))
				if group[0] not in total_entropy:
					total_entropy[group[0]] = entropies_and_elems
				else:
					total_entropy[group[0]] = entropies_and_elems + total_entropy[group[0]] 
		#print(colored(json.dumps(total_entropy, indent=4, sort_keys=True), 'white'))
		#print(self.len)
		result = {}
		for attribute in total_entropy.items():
			#print('attribute: ', attribute[0])
			acc = 0
			for i in range(0, len(attribute[1])):
				acc += attribute[1][i][0]*attribute[1][i][1]/self.len
			#print('entropy: ', acc)
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
        pass

	

if __name__ == '__main__':
	ID3(sys.argv[1])
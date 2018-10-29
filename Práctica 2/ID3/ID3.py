# Insertar aqui la cabecera
import json
import csv
import sys
import operator
import random
from termcolor import colored
class ID3Tree():

	def __init__(self, data, attributes):
		self.attributes = attributes
		self.data = data
		major_class, count_class = self.get_major_class()
		self.generate_tree(major_class, count_class)

	def get_major_class(self):
		classes = {}
		for instance in self.data:
			class_value = instance.get('class')
			if class_value in classes:
				classes[class_value] = 1 + classes[class_value]
			else:
				classes[class_value] = 1

		for i in classes.items():
			print(i)
		return max(classes.items(), key=operator.itemgetter(1))[0], len(classes)

	def group_by_class(self):
		result = {}
		for attribute in self.attributes:
			grouped_instances = {}
			print(colored(attribute, 'red'))
			for instance in self.data:
				if instance[attribute] in grouped_instances:
					grouped_instances[instance[attribute]] = [instance] + grouped_instances[instance[attribute]]
				else:
					grouped_instances[instance[attribute]] = [instance]
			result[attribute] = grouped_instances

		return result

	def generate_tree(self, major_class, count_class):
		if count_class == 1 or len(self.attributes) == 0:
			print("new hoja(max(classes))")
		else: 
			grouped_instances = self.group_by_class() # agrupamos por clase
			entropy_by_group = self.get_entropy(grouped_instances)
			#print(colored(json.dumps(grouped_instances, indent=4, sort_keys=True), 'yellow'))

	def get_entropy(self, grouped_instances):
		for group in grouped_instances.items():
			print(colored(json.dumps(group, indent=4, sort_keys=True), 'blue'))
		return 0



class ID3(object):

    def __init__(self, fichero):
    	attributes = []
    	data = []
    	with open(fichero) as input_file:
    		line = csv.reader(input_file, delimiter = ',')
    		attributes = next(line)
    		for word in line:
    			data.append({attributes[i] : word[i] for i in range(0, len(word))})
    		self.tree = ID3Tree(data, attributes)


    def clasifica(self, instancia):
        pass
        
        
    def test(self, fichero):
        pass


    def save_tree(self, fichero):
        pass

	

if __name__ == '__main__':
	ID3(sys.argv[1])
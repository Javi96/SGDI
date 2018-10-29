# Insertar aqui la cabecera
import json
import csv
import sys
import operator

class ID3Tree():

	def __init__(self, data, attributes):
		#print(data, attributes)

		''' ['buying', 'maint', 'doors', 'persons', 'lug_boot', 'safety', 'class']
		['vhigh', 'low', '5more', '2', 'big', 'high', 'unacc']
		['low', 'vhigh', '3', 'more', 'med', 'low', 'unacc']
		['vhigh', 'low', '5more', '2', 'med', 'low', 'unacc']
		['vhigh', 'high', '3', 'more', 'small', 'low', 'unacc']
		['med', 'low', '4', '4', 'big', 'low', 'unacc']
		['vhigh', 'high', '2', '2', 'med', 'low', 'unacc']
		['med', 'med', '3', 'more', 'small', 'high', 'acc']
		['high', 'low', '5more', '2', 'small', 'high', 'unacc']
		['med', 'vhigh', '4', 'more', 'med', 'med', 'acc']
		['vhigh', 'vhigh', '4', '4', 'small', 'med', 'unacc']
		['low', 'med', '5more', 'more', 'med', 'high', 'vgood']
		['high', 'high', '2', '4', 'med', 'low', 'unacc']
		['high', 'med', '2', '4', 'small', 'high', 'acc']
		['low', 'vhigh', '4', '4', 'big', 'med', 'acc']
		'''
		self.attributes = attributes
		self.data = data

	def get_major_class(self):
		classes = {}
		for instance in self.data:
			class_value = instance[-1]
			if class_value in classes:
				classes[class_value] = 1 + classes[class_value]
			else:
				classes[class_value] = 1
		for i in classes.items():
			print(i)

		return max(classes.items(), key=operator.itemgetter(1))[0], len(classes)

	def generate_tree(self):
		major_class, count_class = self.get_major_class()
		if count_class == 1 or len(self.attributes) == 0: 
			print("new hoja(max(classes))")
		else: print('bifurcamos\nmajor class: ', major_class)




class ID3(object):

    def __init__(self, fichero):
    	attributes = []
    	data = [] 
    	with open(fichero) as input_file:
    		line = csv.reader(input_file, delimiter=',')
    		attributes = next(line)
    		print(attributes)
    		for word in line:
    			data.append(word)
    			print(word)
    	self.tree = ID3Tree(data, attributes)


    def clasifica(self, instancia):
        pass
        
        
    def test(self, fichero):
        pass


    def save_tree(self, fichero):
        pass

	

if __name__ == '__main__':
	ID3(sys.argv[1]).tree.generate_tree()
'''JoseJavierCortesTejada y AitorCayonRuano declaramos que esta solución
es fruto exclusivamente de nuestro trabajo personal. No hemos sido
ayudados por ninguna otra persona ni hemos obtenido la solución de
fuentes externas, y tampoco hemos compartido nuestra solución con
nadie. Declaramos además que no hemos realizado de manera desho-
nesta ninguna otra actividad que pueda mejorar nuestros resultados
ni perjudicar los resultados de los demás.'''

from math import log2, sqrt, pow
from operator import mul, itemgetter

V = ['el', 'es', 'hombre', 'la', 'maude', 'ortiga' , 'patata', 'rica']
d = ['la patata es rica', 'el hombre la ortiga', 'hombre maude la patata rica']
q = ['maude patata', 'la ortiga', 'rica']

# CALCULO DE f
fij = [[],[],[]]
for x in range(0, len(d)):
    for y in range(0, len(V)):
        fij[x].append(d[x].count(V[y]))

for x in range(0, len(d)):
    print('fij' + str(x) + ": " + str(fij[x]) + '\n')

#CALCULO DE tf
tfij = [[],[],[]]
for x in range(0, len(d)):
    for y in range(0, len(V)):
        if fij[x][y] == 0:
            tfij[x].append(0)
        else:
            tfij[x].append(1 + log2(fij[x][y]))

for x in range(0, len(d)):
    print('tfij' + str(x) + ": " + str(tfij[x]) + '\n')

#CALCULO DEL NUMERO DE DOCS EN LOS QUE APARECE UN TERMINO
ni = []
for x in range(0, len(V)):
    apariciones = 0
    for y in range(0, len(d)):
        apariciones = apariciones + (V[x] in d[y])
    ni.append(apariciones)
print('ni: ' + str(ni) + '\n')

#CALCULO DEL idf
idfi = []
for x in range(0, len(ni)):
    idfi.append(log2(len(d)/ni[x]))
print('idfi: ' + str(idfi) + '\n')

# CALCULO DEL wij
wij = [[],[],[]]
for x in range(0, len(d)):
    for y in range(0, len(V)):
        wij[x].append(tfij[x][y]*idfi[y])

for x in range(0, len(d)):
    print('d' + str(x) + ": " + str(wij[x]) + '\n')

# COMPUTO DE LAS PALABRAS QUE APARECEN EN UNA CONSULTA
fiq = [[],[],[]]
for x in range(0, len(q)):
    for y in range(0, len(V)):
        fiq[x].append(int(V[y] in q[x]))

for x in range(0, len(q)):
    print('q' + str(x) + ": " + str(fiq[x]) + '\n')

#CALCULO DE R
R = [[],[],[]]
for x in range(0, len(q)):
    for y in range(0, len(d)):
        a = sum(list(map(mul, wij[y], fiq[x])))
        b = sqrt(sum(list(map(lambda x: pow(x,2), wij[y]))))
        c = sqrt(sum(list(map(lambda x: pow(x,2), fiq[x]))))
        R[x].append(a/(b*c))

for x in range(0, len(q)):
    print('R' + str(x) + ": " + str(R[x]) + '\n')

# RANKING DE LOS DOCUMENTOS EN BASE A UNA CONSULTA
ranking = [[],[],[]]
for x in range(0, len(q)):
	ranking[x] = sorted(list(zip(R[x], d)), key = itemgetter(0), reverse = True)

for x in range(0, len(q)):
	print("\nCONSULTA: " + q[x] + "\nRANKING DE DOCUMENTOS: ")
	for y in range(0, len(ranking[x])):
		print(str(y+1) + ". " + ranking[x][y][1])


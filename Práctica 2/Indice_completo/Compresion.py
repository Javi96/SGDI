
from bitarray import bitarray
from termcolor import colored
import math

def apply_default(complete_index):
    """
    Convierte las listas de posiciones del indice en una lista de diferencias de posiciones.

    Parametros
    ----------
    complete_index : dcit
        Indice invertido completo 

    Retorno
    -------
    tuple
        int
            Bits usados por las listas de diferencias
        dict 
            Indice con listas de diferencias

    """

    bits = 0
    for res in complete_index.items():
        doc_card = list(res[1].keys())[0]
        for elem in res[1][doc_card].items():
            for i in reversed(range(0, len(elem[1][1]))): # empezamos por el final para evitar contadores
                if i != 0:
                    elem[1][1][i] -= elem[1][1][i-1]
            bits += len(elem[1][1])*32

    return (bits, complete_index)
    
def code_unary(positions):
    """
    Aplica la codificacion unario a una lista de posiciones.

    Parametros
    ----------
    positions : list
        Lista de posiciones 

    Retorno
    -------
    tuple
        int
            Bits usados para la codificacion
        bitarray 
            Codificacion de la lista de diferencias

    """

    bit_array = bitarray()
    for i in positions:
        for j in range(0, i-1):
            bit_array.append(1)
        bit_array.append(0)
    return (len(bit_array), bit_array)

def decode_unary(bits):
    """
    Decodifica la codificacion unaria.

    Parametros
    ----------
    bits : bitarray
        Codificacion en unario 

    Retorno
    -------
    list
        Lista de diferencias de posiciones

    """

    count = 1
    result = []
    for bit in bits:
        if bit:
            count += 1
        else:
            result.append(count)
            count = 1
    return result

def code_elias_gamma(positions):
    """
    Aplica la codificacion elias-gamma a una lista de posiciones.

    Parametros
    ----------
    positions : list
        Lista de posiciones 

    Retorno
    -------
    tuple
        int
            Bits usados para la codificacion
        bitarray 
            Codificacion de la lista de diferencias

    """

    result = bitarray()
    for position in positions:
        offset = "{0:b}".format(position)[1:]
        length = code_unary([len(offset)+1])[1]
        result.extend(''.join([length.to01(), offset]))
    return (len(result), result)

def decode_elias_gamma(bits_list):
    """
    Decodifica la codificacion elias-gamma.

    Parametros
    ----------
    bits_list : bitarray
        Codificacion de la lista de diferencia de posiciones 

    Retorno
    -------
    list
        Lista de direfencias de posiciones decodificada

    """

    result = []
    cont = True
    dec_unary = 0
    aux = bitarray('')
    index = 1
    for bit in bits_list:

        
        aux.extend([bit])
        if cont and not bit:
            dec_unary = decode_unary(aux)[0]
            aux = bitarray('1')
            cont = False
        
        if not cont and index != dec_unary:
            index += 1
        elif not cont and index == dec_unary:
            result.append(int(aux.to01(), 2))
            aux = bitarray()
            cont = True
            index = 1
    return result


def code_elias_delta(positions):
    """
    Aplica la codificacion elias-delta a una lista de posiciones.

    Parametros
    ----------
    positions : list
        Lista de posiciones 

    Retorno
    -------
    tuple
        int
            Bits usados para la codificacion
        bitarray 
            Codificacion de la lista de diferencias

    """

    result = bitarray()
    for position in positions:
        offset = "{0:b}".format(position)[1:]
        length = code_elias_gamma([len(offset)+1])
        result.extend(''.join([length[1].to01(), offset]))
    return (len(result), result)

def decode_elias_delta(bits_list):
    """
    Decodifica la codificacion elias-delta.

    Parametros
    ----------
    bits_list : bitarray
        Codificacion de la lista de diferencia de posiciones 

    Retorno
    -------
    list
        Lista de direfencias de posiciones decodificada

    """

    result = []
    ones = 0
    res = 0
    length = 0
    cont = True
    acc = bitarray()
    acc_partial =  bitarray('1')
    partial = 0

    for index in range(0, len(bits_list.to01())): 
        if bits_list[index] and cont:
            ones += 1
            acc.append(True)
        elif not bits_list[index] and cont:
            acc.append(False)
            if len(acc) == 1:
                result.append(1)
                acc = bitarray()
            else:    
                cont = False
            

        elif not cont and partial != ones:
            partial += 1
            acc.extend([bits_list[index]])
            if partial == ones:
                length = int(decode_elias_gamma(acc)[0])
        elif partial == ones:
            if res < length-1:
                print(colored(acc_partial, 'green'))
                res += 1
                acc_partial.extend([bits_list[index]])
            if res == length-1:
                result.append(int(acc_partial.to01(), 2))
                acc = bitarray()
                acc_partial =  bitarray('1')
                ones = 0
                res = 0
                partial = 0
                cont = True

    return result

def code_variable_bits(data):
    """
    Aplica la codificacion de bytes variables a una lista de posiciones.

    Parametros
    ----------
    positions : list
        Lista de posiciones 

    Retorno
    -------
    tuple
        int
            Bits usados para la codificacion
        bitarray 
            Codificacion de la lista de diferencias

    """

    result = bitarray()
    for i in data:
        acc = 0
        length = len("{0:b}".format(i))
        

        offset = length%7
        binary = list("{0:b}".format(i))
        offset_bin = binary[0:offset]

        if length < 7:
            result.extend('1'+str(''.join(offset_bin)).zfill(7)) # caso pequeño
        else:
            binary = binary[offset:]
            aux = str(''.join(offset_bin)).zfill(8)
            result.extend(aux) # caso pequeño
            length = len(binary)
            for j in range(0, int(length/7))[:-1]:
                aux = binary[j*7:(j+1)*7]
                result.extend(str(''.join(aux)).zfill(8))
            result.extend('1'+str(''.join(binary[length-7:length])).zfill(7))
    return (len(result), result)

def decode_variable_bits(data):
    """
    Decodifica la codificacion de bytes variables.

    Parametros
    ----------
    data : bitarray
        Codificacion de la lista de diferencia de posiciones 

    Retorno
    -------
    list
        Lista de direfencias de posiciones decodificada

    """

    result = []
    new_data = data.to01()
    length = int(len(new_data)/8)

    acc = bitarray()
    for index in range(0, length):
        sub_sec = new_data[index*8:(index+1)*8]
        acc.extend(sub_sec[1:])
        if sub_sec[0] is '1':
            result.append(int(acc.to01(), 2))
            acc = bitarray()
    return result



if __name__ == '__main__':
    # ejemplos de ejecucion
    data = []
    for i in range(0,50000):
        data.append(i)

    result = code_variable_bits(data)
    res = decode_variable_bits(result[1])

    for i in range(0, len(data)):
        if data[i] != res[i]:
            print('mal', data[i], res[i])

    print('boi')
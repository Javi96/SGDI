
from bitarray import bitarray
from termcolor import colored
import math

def apply_default(result):
    for res in result.items():
        #print(list(res[1].keys()))
        #print(res[1])
        doc_card = list(res[1].keys())[0]
        for elem in res[1][doc_card].items():
            #print('\t',elem)
            for i in reversed(range(0, len(elem[1][1]))):
                if i != 0:
                    elem[1][1][i] -= elem[1][1][i-1]
                    #print(i)
    return result

def unary(result):
    #print(colored('unary', 'green'))
    for res in result.items():
        #print(list(res[1].keys()))
        #print(res[1])
        doc_card = list(res[1].keys())[0]
        for elem in res[1][doc_card].items():
            #print('\t',elem)
            bit_array = code_unary(elem[1][1])
            occurence_card = elem[1][0]
            res[1][doc_card][elem[0]] = (occurence_card, bit_array)

    #print(colored(result, 'green'))
    return result
    
def code_unary(positions):
    #print('code_unary', 'yellow')
    bit_array = bitarray()
    for i in positions:
        for j in range(0, i-1):
            bit_array.append(1)
        bit_array.append(0)

        #print(colored(bit_array, 'blue'))
    return bit_array

def decode_unary(bits):
    #print('decode_unary', 'yellow')
    count = 1
    result = []
    for bit in bits:
        if bit:
            count += 1
        else:
            result.append(count)
            count = 1
    #print(colored(bits, 'yellow'))
    #print(colored(result, 'yellow'))
    return result

def variable_bytes(result):
    print('boiiiiii')

def code_elias_gamma(positions):
    codec = []
    for value in positions:
        #print('\tlista --> ',elem)
        offset = "{0:b}".format(value)[1:]
        length = code_unary([len(offset)+1])
        #print('binario: ', "{0:b}".format(value))
        #print('offset: ', offset, '\nlength: ', length.to01(), '\nelias_gamma: ', ''.join([length.to01(), offset]))
        #print(bitarray(''.join([length.to01(), offset])))
        codec.append(bitarray(''.join([length.to01(), offset])))
    return (len(codec), codec)

def elias_gamma(result):

    #print(colored('elias_gamma', 'yellow'))

    for res in result.items():
        doc_card = list(res[1].keys())[0]
        for elem in res[1][doc_card].items():
            res[1][doc_card][elem[0]] = code_elias_gamma(elem[1][1])
    return result


def decode_elias_gamma(bits_list):
    #print('decode_elias_gamma')
    result = []
    for bit in bits_list:
        #print(colored(bit, 'yellow'))
        a = 0
        aux_str = ['1']
        cont = True
        for i in range(0, len(bit.to01())):
            #print(bit.to01()[i])
            if cont and bit.to01()[i] is '1':
                a += 1
            elif cont and bit.to01()[i] is '0':
                
                for j in range(i+1, len(bit.to01())):
                    #print('index: ', j)
                    aux_str.append(bit.to01()[j])
                cont = False
            else:
                break
        #print(colored(aux_str, 'blue'))
        #print(int(''.join(aux_str)))
        #print(type(int(''.join(aux_str))))
        result.append(int(''.join(aux_str), 2))
    #print(result)
    return result

def code_elias_delta(positions):
    codec = []
    print(colored(positions, 'green'))
    for value in positions:
        offset = "{0:b}".format(value)[1:]
        print(offset)
        a = len("{0:b}".format(value))
        print('valor: ', value)
        print('longitud en binario: ', a)

        r = code_elias_gamma([a])
        print(a, r)
        codec.append(bitarray(''.join([r[1][0].to01(), offset])))
    return (len(codec), codec)

def elias_delta(result):
    for res in result.items():
        doc_card = list(res[1].keys())[0]
        for elem in res[1][doc_card].items():
            res[1][doc_card][elem[0]] = code_elias_delta(elem[1][1])
    return result

def decode_elias_delta(bit_list):
    print(colored('decode_elias_delta', 'blue'))
    print(bit_list)
    result = []
    for bit_array in bit_list:
        ones = 0
        cont = True
        acc = bitarray()
        print(bit_array)
        partial = 0
        for index in range(0, len(bit_array.to01())):
            print(bit_array[index])
            
            if bit_array[index] and cont:
                ones += 1
            elif not bit_array[index] and cont:
                cont = False
            
            if not cont:
                partial += 1
            acc.append(bit_array[index])

            if partial == ones+1:
                print(acc)
                l = int(decode_elias_gamma([acc])[0])
                print('DECODE: ', decode_elias_gamma([acc]))
                print('longitud bin: ', int(decode_elias_gamma([acc])[0]))
                print('numero en bin: ', '1'+bit_array.to01()[index-1:l])
                data = str('1'+bit_array.to01()[index-1:l])
                result.append(int(data, 2))
    print('RES: ', result)
        
    return result


def variable_bits(data):
    result = bitarray()
    for i in data:
        print('value: ', i)
        print('binar: ', "{0:b}".format(i))
        print('len:   ', len("{0:b}".format(i)))
        acc = 0
        length = len("{0:b}".format(i))
        print('rever: ', list(range(0, int(length/7))))
        print('rever: ', list(range(0, int(length/7)))[::-1])
        

        offset = length%7
        binary = list("{0:b}".format(i))
        offset_bin = binary[0:offset]

        if length <= 7:
            print('1'+str(''.join(offset_bin)).zfill(7))
            result.extend('1'+str(''.join(offset_bin)).zfill(7)) # caso pequeño
        else:
            binary = binary[offset:]
            print('index offset bin: ', str(''.join(offset_bin)).zfill(8))
            aux = str(''.join(offset_bin)).zfill(8)
            result.extend(aux) # caso pequeño
            print('ba: ', result)
            length = len(binary)
            for j in range(0, int(length/7))[:-1]:
                aux = binary[j*7:(j+1)*7]
                print(colored(''.join(aux), 'red'))
                result.extend(str(''.join(aux)).zfill(8))
            result.extend('1'+str(''.join(binary[length-7:length])).zfill(7))
        print(result)
    return result

def decode_variable_bits(data):
    print(colored('decode_variable_bits', 'yellow'))
    result = []
    new_data = data.to01()
    length = int(len(new_data)/8)
    print(length)
    print(colored(new_data, 'blue'))

    acc = bitarray()
    for index in range(0, length):
        sub_sec = new_data[index*8:(index+1)*8]
        print(colored(sub_sec, 'red'))
        acc.extend(sub_sec[1:])
        if sub_sec[0] is '1':
            print('soy 1, acabo', acc)
            result.append(int(acc.to01(), 2))
            acc = bitarray()
        print(new_data[index*8:(index+1)*8])
        print(index)
    print(result)
        















def new_elias_gamma(positions):
    result = bitarray()
    for position in positions:
        offset = "{0:b}".format(position)[1:]
        length = code_unary([len(offset)+1])
        result.extend(''.join([length.to01(), offset]))
    return (len(result), result)

def new_decode_elias_gamma(bits_list):
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


def new_elias_delta(positions):
    result = bitarray()
    for position in positions:
        offset = "{0:b}".format(position)[1:]
        length = new_elias_gamma([len(offset)+1])
        print(length)
        result.extend(''.join([length[1].to01(), offset]))
    return (len(result), result)

def new_decode_elias_delta(bits_list):
    result = []
    ones = 0
    res = 0
    length = 0
    cont = True
    acc = bitarray()
    acc_partial =  bitarray('1')
    partial = 0
    print(colored(bits_list, 'yellow'))

    for index in range(0, len(bits_list.to01())): 
        print(acc)
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
                length = int(new_decode_elias_gamma(acc)[0])
        elif partial == ones:
            print(acc)
            print(new_decode_elias_gamma(acc))
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



if __name__ == '__main__':
    '''result = variable_bits([5, 315, 458965, 41523654])
    print(result)
    data = decode_variable_bits(result)'''
    result = new_elias_delta([5,1,2,5,4,9565,65324])
    print(result)
    result = new_decode_elias_delta(result[1])
    print(result)
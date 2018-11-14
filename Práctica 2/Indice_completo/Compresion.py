
from bitarray import bitarray
from termcolor import colored
import math
def show_out():
        print('papu')

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

def elias_gamma(result):

    print(colored('elias_gamma', 'yellow'))

    for res in result.items():
        doc_card = list(res[1].keys())[0]
        for elem in res[1][doc_card].items():
            codec = []
            for value in elem[1][1]:
                print('\tlista --> ',elem)
                offset = "{0:b}".format(value)[1:]
                length = code_unary([len(offset)+1])
                print(int(length.to01()))
                print('offset: ', offset, '\nlength: ', length.to01(), '\nelias_gamma: ', ''.join([length.to01(), offset]))
                print(bitarray(''.join([length.to01(), offset])))
                codec.append(bitarray(''.join([length.to01(), offset])))
            res[1][doc_card][elem[0]] = (len(codec), codec)
    return result

def decode_elias_gamma(bits_list):
    print(colored('decode_elias_gamma', 'yellow'))
    print("{0:b}".format(5))
    print(bits_list)
    result = []
    for bits in bits_list:
        acc = 0
        index = 0
        value = 0
        cont = True
        for bit in bits:
            if bit and cont:
                acc += 1
            elif not bit and cont:
                value = math.pow(2, acc)
                print(acc, value)
                acc = 0
                cont = False 
            elif bit:
                acc += 1
        value += acc

        print(bits, '\t>>>\b', value)
        result.append(int(value))
    return result

def elias_delta(self):
    print('boiiiiii')
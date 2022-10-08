#computing a list of max from a key/value input
# input :
#     * a filename to a file with the list of couple of letter(key) and an integer (value) as (a,3) separate by ';' # output 
#     * n number of max to return
# ouput :
#     the list key of the keys of the n max value as python list [a,f,3]
# the resolution must be done in less time than the naive implemented here on big files tests
#____________
# command : max 
# args :
#   - pathfile 
#   - n
# n must be changed to 1 of negative or null  
# the naive algorithm implemeted 
# run is the function called by the test runner
import heapq

def get_x_max(path,n):
    nbmax=int(n)
    if nbmax<1: 
        nbmax=1  

    flist=open(path,'r')
    s=flist.read()
    nlargest = max_in_list(s, n)
    return str([pair[0] for pair in nlargest])

def max_in_list(s, n):
    pairs=[pair[1:-1].split(',') for pair in s.split(';')]
    # print(pairs)
    return heapq.nlargest(n, pairs, key=lambda x:int(x[1]))
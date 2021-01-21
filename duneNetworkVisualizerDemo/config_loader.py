#!/usr/bin/env python

def load_config(file_in="filter.cfg"):
    #Gets our keypairs
    f = open(file_in, "r")
    lines = f.readlines()
    f.close()

    #Splits our keypairs into, well, keys and their possible values
    splitlines = []
    splitlines.append(line.split(':') for line in lines)

    #Sets up a dictionary of all our config settings
    config = {splitlines[i][0]:splitlines[i][1].split(',') for i in range(len(splitlines))}

    return config

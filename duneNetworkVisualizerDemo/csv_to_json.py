import csv
import dict_filters.py

#Sets up an array for the dictionaries being made
data_dicts = []

#Opens our file and csv reader
f = open('csv_in.csv')
reader = csv.reader(f, delimiter=',')

#Loads in our keys
keys = reader.__next__()

#Appends our dictionaries
for row in reader:
    data_dicts.append({keys[i]:row[i] for i in range(len(row))})
f.close()

if __name__ == "__main__":
    

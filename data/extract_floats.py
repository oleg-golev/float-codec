#!/usr/bin/env python
import csv
import numpy as np


def is_float(value):
    try:
        float(value)
        return True
    except:
        return False


def extract_floats():
    floats = []
    with open('WS_LBS_D_PUB_csv_col.csv', newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
        for row in reader:
            for collection in row:
                elements = collection.split(",")
                for element in elements:
                    el = element.strip("\"")
                    if is_float(el):
                        floats.append(float(el))
    with open('floats.txt', 'w') as f:
        for fl in floats:
            f.write("%s\n" % fl)


def read_floats():
    file = open("floats.txt", "r")
    content = file.read()
    print(content)
    file.close()


def main():
    extract_floats()
    # read_floats()


if __name__ == "__main__":
    main()

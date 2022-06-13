#!/usr/bin/env python
import csv
import numpy as np
import math


def is_float(value):
    try:
        float(value)
        return True
    except:
        return False


def extract_floats():
    floats = []
    with open('./data/WS_LBS_D_PUB_csv_col.csv', newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
        for row in reader:
            for collection in row:
                elements = collection.split(",")
                for element in elements:
                    el = element.strip("\"")
                    if is_float(el):
                        elf = float(el)
                        if not math.isnan(elf):
                            floats.append(float(el))
    with open('./data/floats.txt', 'w') as f:
        for fl in floats:
            f.write("%s\n" % fl)


def extract_aapl():
    closel = []
    openl = []
    highl = []
    lowl = []
    with open("./data/HistoricalData_1654792445080.csv", newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
        first = True
        for row in reader:
            if first:
                first = False
                continue
            for collection in row:
                elements = collection.split(",")
                closel.append(elements[1].strip("\"$"))
                openl.append(elements[3].strip("\"$"))
                highl.append(elements[4].strip("\"$"))
                lowl.append(elements[5].strip("\"$"))
                print(elements)
    write_to_file("close", closel)
    write_to_file("open", openl)
    write_to_file("high", highl)
    write_to_file("low", lowl)


def write_to_file(name, arr):
    namel = './data/{}.txt'.format(name)
    with open(namel, 'w') as f:
        for fl in arr:
            f.write("%s\n" % fl)


def read_floats():
    file = open("./data/floats.txt", "r")
    content = file.read()
    print(content)
    file.close()


# def main():
#     # extract_floats()
#     # read_floats()
#     extract_aapl()


# if __name__ == "__main__":
#     main()

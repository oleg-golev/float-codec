import pickle
import gorillacompression as gc
from objsize import get_deep_size
import math
import time
import csv

import h5py
import numpy
import bitshuffle.h5


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
    return floats


def main():
    array = extract_floats()
    num_floats = len(array)
    print(h5py.__version__)  # >= '2.5.0'

    f = h5py.File("compressed", "w")

    # block_size = 0 let Bitshuffle choose its value
    block_size = 0
    dataset = f.create_dataset(
        "data",
        (num_floats),
        compression=bitshuffle.h5.H5FILTER,
        compression_opts=(block_size, bitshuffle.h5.H5_COMPRESS_LZ4),
        dtype='float64',
    )

    # start = time.time()
    # # encoded = gc.ValuesEncoder.encode_all(floats)
    # enc_time = time.time()
    # print("encoding time:", enc_time - start)

    # decoded = gc.ValuesDecoder.decode_all(encoded)
    # dec_time = time.time()

    # print("decoding time:", dec_time - enc_time)

    dataset[:] = array

    f.close()

    file = open("compressed", "rb")
    byte = file.read(1)
    count = 0
    while byte:
        count += 1
        byte = file.read(1)

    print("went from", len(pickle.dumps(array)), "to", count,
          ", ratio =", len(pickle.dumps(array)) / count)


if __name__ == "__main__":
    main()

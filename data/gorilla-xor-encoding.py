import pickle
import gorillacompression as gc
from objsize import get_deep_size
import math
import time
import csv


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
    floats = extract_floats()[int(65535/8):2*int(65535/8)]
    num_floats = len(floats)

    start = time.time()
    encoded = gc.ValuesEncoder.encode_all(floats)
    enc_time = time.time()
    print("encoding time:", enc_time - start)

    print("went from", len(pickle.dumps(floats)), "to", len(pickle.dumps(encoded)),
          ", ratio =", len(pickle.dumps(floats)) / len(pickle.dumps(encoded)))
    decoded = gc.ValuesDecoder.decode_all(encoded)
    dec_time = time.time()

    print("decoding time:", dec_time - enc_time)


if __name__ == "__main__":
    main()

import os
import subprocess

import yaml


IN_TXT_FILE =  os.getenv("in_txt_file")
OUT_TXT_FILE = os.getenv("out_txt_file")
OUT_METADATA_FILE = os.getenv("out_metdata_file")
IN_TXT_FOLDER = "/veld/input/"
OUT_TXT_FOLDER = "/veld/output/data/"
OUT_METADATA_FOLDER = "/veld/output/metadata/"
OUT_DATA_DESCRIPTION = os.getenv("out_data_description")
if IN_TXT_FILE and OUT_TXT_FILE:
    print(f"IN_TXT_FILE: {IN_TXT_FILE}")
    print(f"OUT_TXT_FILE: {OUT_TXT_FILE}")
elif IN_TXT_FILE or OUT_TXT_FILE:
    raise Exception(
        "Either 'in_txt_file' or 'out_txt_file' param is set, but not both. This is an unclear "
        "instruction. Must be both set or neither."
    )
if OUT_METADATA_FILE:
    print("OUT_METADATA_FILE:", OUT_METADATA_FILE)


def make_lowercase_individual(in_txt_file_path, out_txt_file_path):
    with open(in_txt_file_path, "r") as f_in:
        with open(out_txt_file_path, "w") as f_out:
            for line in f_in:
               f_out.write(line.lower())


def make_lowercase():
    print("begin making all text lowercase.")
    if IN_TXT_FILE and OUT_TXT_FILE:
        make_lowercase_individual(IN_TXT_FOLDER + IN_TXT_FILE, OUT_TXT_FOLDER + OUT_TXT_FILE)
    else:
        for file in os.listdir(IN_TXT_FOLDER):
            make_lowercase_individual(IN_TXT_FOLDER + file, OUT_TXT_FOLDER + file)
    print("done")



def count_lines(file_path):
    result = subprocess.run(["wc", "-l", file_path], capture_output=True, text=True)
    num_lines = int(result.stdout.split()[0])
    return num_lines


def write_veld_data_yaml():
    result = subprocess.run(["du", "-sh", OUT_TXT_FOLDER], capture_output=True, text=True)
    data_size = result.stdout.split()[0]
    if OUT_TXT_FILE:
        num_lines = count_lines(OUT_TXT_FOLDER + OUT_TXT_FILE)
    else:
        num_lines = 0
        for file in os.listdir(OUT_TXT_FOLDER):
            num_lines += count_lines(OUT_TXT_FOLDER + file)
    veld_data_yaml = {
        "x-veld": {
            "data": {
                "description": OUT_DATA_DESCRIPTION,
                "topics": "NLP",
                "contents": [
                    "training data",
                    "raw text",
                ],
                "file_type": "txt",
                "additional": {
                    "data size": data_size,
                    "number of lines": num_lines,
                }
            }
        }
    }
    veld_data_yaml["x-veld"]["data"]["additional"]["data size"] = data_size
    with open(OUT_METADATA_FOLDER + OUT_METADATA_FILE, "w") as f:
        yaml.dump(veld_data_yaml, f, sort_keys=False)


def main():
    make_lowercase()
    if OUT_METADATA_FILE:
        write_veld_data_yaml()


if __name__ == "__main__":
    main()


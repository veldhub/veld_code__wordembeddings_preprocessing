import os


INPUT_FILE_PATH = "/veld/input/" + os.getenv("in_file")
OUTPUT_FILE_PATH = "/veld/output/" + os.getenv("out_file")


with open(INPUT_FILE_PATH, "r") as f_in:
    with open(OUTPUT_FILE_PATH, "w") as f_out:
        for line in f_in:
            f_out.write(line.lower())


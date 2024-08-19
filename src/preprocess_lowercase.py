import os


INPUT_FILE_PATH = os.getenv("input_file_path")
OUTPUT_FILE_PATH = os.getenv("output_file_path")


with open(INPUT_FILE_PATH, "r") as f_in:
    with open(OUTPUT_FILE_PATH, "w") as f_out:
        for line in f_in:
            f_out.write(line.lower())


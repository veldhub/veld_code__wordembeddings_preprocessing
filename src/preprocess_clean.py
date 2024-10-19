import os
import subprocess
import unicodedata

import yaml

from common import multi_process


INPUT_FILE_PATH = "/veld/input/" + os.getenv("in_file")
OUTPUT_FILE_CLEAN_PATH = "/veld/output/1/" + os.getenv("out_file_clean")
OUTPUT_FILE_DIRTY_PATH = "/veld/output/1/" + os.getenv("out_file_dirty")
OUT_TMP_FOLDER_CLEAN = "/veld/output/2/clean/"
OUT_TMP_FOLDER_DIRTY = "/veld/output/2/dirty/"
OUT_VELD_DATA_YAML_PATH = "/veld/output/veld_data_cleaned.yaml"
MIN_PERCENTAGE_CHAR = float(os.getenv("min_percentage_char"))
OUT_DATA_DESCRIPTION = os.getenv("out_data_description")
CPU_COUNT = os.getenv("cpu_count")
if CPU_COUNT is None:
    CPU_COUNT = os.cpu_count()
else:
    CPU_COUNT = int(CPU_COUNT)
    if CPU_COUNT > os.cpu_count():
        CPU_COUNT = os.cpu_count()
BUFFER_SEGMENTS = int(os.getenv("buffer_segments"))


def is_letter(char):
    return unicodedata.category(char).startswith('L')


def process_line(line, should_write_clean):
    count_clean = 0
    count_dirty = 0
    for char in line:
        cat = unicodedata.category(char)
        if cat.startswith("L") or cat.startswith("Z"):
            count_clean += 1
        else:
            count_dirty += 1
    percentage_char = (100 * count_clean)  / (count_clean + count_dirty)
    is_line_clean = percentage_char >= MIN_PERCENTAGE_CHAR
    if (is_line_clean and should_write_clean) or (not is_line_clean and not should_write_clean):
        line_to_write = line
    elif (is_line_clean and not should_write_clean) or (not is_line_clean and should_write_clean):
        line_to_write = ""
    return line_to_write


def process_line_clean(line):
    return process_line(line, True)


def process_line_dirty(line):
    return process_line(line, False)


def write_veld_data_yaml():
    data_size = subprocess.run(["du", "-sh", OUTPUT_FILE_CLEAN_PATH], capture_output=True, text=True)
    data_size = data_size.stdout.split()[0]
    num_lines_cleaned = subprocess.run(["wc", "-l", OUTPUT_FILE_CLEAN_PATH], capture_output=True, text=True)
    num_lines_cleaned = num_lines_cleaned.stdout.split()[0]
    num_lines_dirty = subprocess.run(["wc", "-l", OUTPUT_FILE_DIRTY_PATH], capture_output=True, text=True)
    num_lines_dirty = num_lines_dirty.stdout.split()[0]
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
                "path": "data.txt",
                "additional": {
                    "data size": data_size,
                    "number of clean lines": num_lines_cleaned,
                    "number of dirty removed lines": num_lines_dirty,
                    "minimum number of characters in percentage of each line": MIN_PERCENTAGE_CHAR
                }
            }
        }
    }
    with open(OUT_VELD_DATA_YAML_PATH, "w") as f:
        yaml.dump(veld_data_yaml, f, sort_keys=False)


def main():
    print(f"INPUT_FILE_PATH: {INPUT_FILE_PATH}")
    print(f"OUTPUT_FILE_CLEAN_PATH: {OUTPUT_FILE_CLEAN_PATH}")
    print(f"OUTPUT_FILE_DIRTY_PATH: {OUTPUT_FILE_DIRTY_PATH}")
    print(f"MIN_PERCENTAGE_CHAR: {MIN_PERCENTAGE_CHAR}")
    print(f"CPU_COUNT: {CPU_COUNT}")
    multi_process(
        cpu_cores=CPU_COUNT, 
        in_file_path=INPUT_FILE_PATH,
        out_file_path=OUTPUT_FILE_CLEAN_PATH,
        out_tmp_folder=OUT_TMP_FOLDER_CLEAN,
        single_line_function=process_line_clean,
        buffer_segments=BUFFER_SEGMENTS,
        sleep_duration=60,
    )
    multi_process(
        cpu_cores=CPU_COUNT, 
        in_file_path=INPUT_FILE_PATH,
        out_file_path=OUTPUT_FILE_DIRTY_PATH,
        out_tmp_folder=OUT_TMP_FOLDER_DIRTY,
        single_line_function=process_line_dirty,
        buffer_segments=BUFFER_SEGMENTS,
        sleep_duration=60,
    )
    write_veld_data_yaml()


if __name__ == "__main__":
    main()


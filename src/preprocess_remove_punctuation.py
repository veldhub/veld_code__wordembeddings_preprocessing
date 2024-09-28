import json
import random
import os
import subprocess
import multiprocessing

import spacy
import yaml

from common import multi_process


IN_TXT_PATH = "/veld/input/" + os.getenv("in_txt_file")
OUT_TXT_PATH = "/veld/output/" + os.getenv("out_txt_file")
OUT_VELD_DATA_YAML_PATH = "/veld/output/veld_data_removed_punctuation.yaml"
OUT_DATA_DESCRIPTION = os.getenv("out_data_description")
TMP_FILE_FOLDER = "/tmp"
CPU_COUNT = os.getenv("cpu_count")
if CPU_COUNT is None:
    CPU_COUNT = os.cpu_count()
else:
    CPU_COUNT = int(CPU_COUNT)
    if CPU_COUNT > os.cpu_count():
        CPU_COUNT = os.cpu_count()
BUFFER_SEGMENTS = int(os.getenv("buffer_segments"))


nlp = spacy.load("de_core_news_lg")


def print_and_log(msg):
    print(msg, flush=True)
    with open("/tmp/log.txt", "a") as out:
        out.write(msg + "\n")


def write_veld_data_yaml():
    result = subprocess.run(["du", "-sh", OUT_TXT_PATH], capture_output=True, text=True)
    data_size = result.stdout.split()[0]
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
                }
            }
        }
    }
    with open(OUT_VELD_DATA_YAML_PATH, "w") as f:
        yaml.dump(veld_data_yaml, f, sort_keys=False)


def process_line(line):
    doc = nlp(line)
    token_non_punct = []
    for token in doc:
        if not token.is_punct:
            token_non_punct.append(token.text)
    sentence_cleaned = " ".join(token_non_punct)
    return sentence_cleaned


def main():
    multi_process(
        cpu_cores=CPU_COUNT, 
        in_file_path=IN_TXT_PATH,
        out_file_path=OUT_TXT_PATH,
        single_line_function=process_line,
        buffer_segments=BUFFER_SEGMENTS,
        sleep_duration=60,
    )
    write_veld_data_yaml()


if __name__ == "__main__":
    print_and_log(f"IN_TXT_PATH: {IN_TXT_PATH}")
    print_and_log(f"OUT_TXT_PATH: {OUT_TXT_PATH}")
    print_and_log(f"CPU_COUNT: {CPU_COUNT}")
    print_and_log(f"BUFFER_SEGMENTS: {BUFFER_SEGMENTS}")
    main()


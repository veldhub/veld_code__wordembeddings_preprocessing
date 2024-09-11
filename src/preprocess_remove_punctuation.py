import json
import random
import os
import subprocess
import multiprocessing

import spacy
import yaml


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
INFO_INTERVAL = int(os.getenv("info_interval"))
print(f"IN_TXT_PATH: {IN_TXT_PATH}")
print(f"OUT_TXT_PATH: {OUT_TXT_PATH}")
print(f"CPU_COUNT: {CPU_COUNT}")
print(f"INFO_INTERVAL: {INFO_INTERVAL}")


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
                "data size": None,
            }
        }
    }
}
nlp = spacy.load("de_core_news_lg")


def get_num_lines():
    print("counting lines of file.")
    result = subprocess.run(["wc", "-l", IN_TXT_PATH], capture_output=True, text=True)
    num_lines = int(result.stdout.split()[0])
    print(f"done. number of lines: {num_lines}")
    return num_lines


def get_interval_index_list(num_lines, num_intervals):
    step = int(round(num_lines / num_intervals))
    interval_index_list = []
    for i in range(1, num_intervals + 1):
        if i < num_intervals:
            interval_index_list.append(step * i)
        else:
            interval_index_list.append(num_lines - 1)
    return interval_index_list


def get_index_start_end_list(num_lines, num_intervals):
    interval_index_list = get_interval_index_list(num_lines, num_intervals)
    index_start_end_list = []
    index_start = 0
    for index_end in interval_index_list:
        index_start_end_list.append((index_start, index_end))
        index_start = index_end + 1
    return index_start_end_list


def run_multi_process(index_start_end_list, tmp_file_list):

    def process_file(tmp_file_path, p_id, index_start_end):
        print(f"process {p_id}: start", flush=True)
        with open(IN_TXT_PATH, "r") as f_in:
            with open(tmp_file_path, "w") as f_out:
                interval_index_list = get_interval_index_list(\
                    index_start_end[1] - index_start_end[0] + 1, INFO_INTERVAL)
                interval_index_list = [i + index_start_end[0] for i in interval_index_list]
                for i, sentence in enumerate(f_in):
                    if i >= index_start_end[0] and i <= index_start_end[1]:
                        doc = nlp(sentence)
                        token_non_punct = []
                        for token in doc:
                            if not token.is_punct:
                                token_non_punct.append(token.text)
                        sentence_cleaned = " ".join(token_non_punct)
                        f_out.write(sentence_cleaned)
                    if i in interval_index_list:
                        print(f"process {p_id}: done with {i - index_start_end[0] + 1} lines out"\
                            f" of {index_start_end[1] - index_start_end[0] + 1}.", flush=True)
        print(f"process {p_id}: done", flush=True)

    process_list = []
    for p_id, index_start_end in enumerate(index_start_end_list):
        process_list.append(multiprocessing.Process(target=process_file, args=(\
            tmp_file_list[p_id], p_id, index_start_end)))
    for process in process_list:
        process.start()
    for process in process_list:
        process.join()


def join_tmp_files(tmp_file_list):
    print("joining tmp files into one.", flush=True)
    with open(OUT_TXT_PATH, "w") as f_out:
        for tmp_file_path in tmp_file_list:
            with open(tmp_file_path, "r") as f_in:
                f_out.write(f_in.read())
    print("done")


def write_veld_data_yaml():
    result = subprocess.run(["du", "-sh", OUT_TXT_PATH], capture_output=True, text=True)
    data_size = result.stdout.split()[0]
    veld_data_yaml["x-veld"]["data"]["additional"]["data size"] = data_size
    with open(OUT_VELD_DATA_YAML_PATH, "w") as f:
        yaml.dump(veld_data_yaml, f, sort_keys=False)


def main():
    num_lines = get_num_lines()
    index_start_end_list = get_index_start_end_list(num_lines, CPU_COUNT)
    tmp_file_list = [f"{TMP_FILE_FOLDER}/{i}.txt" for i in range(CPU_COUNT)]
    run_multi_process(index_start_end_list, tmp_file_list)
    join_tmp_files(tmp_file_list)
    write_veld_data_yaml()


if __name__ == "__main__":
    main()


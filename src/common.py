import math
import os
import subprocess
from datetime import datetime
from multiprocessing import Process
from time import sleep

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
if OUT_DATA_DESCRIPTION:
    print("OUT_DATA_DESCRIPTION:", OUT_DATA_DESCRIPTION)



def write_veld_data_yaml():

    def count_lines(file_path):
        result = subprocess.run(["wc", "-l", file_path], capture_output=True, text=True)
        num_lines = int(result.stdout.split()[0])
        return num_lines

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


def multi_process(
    cpu_cores, 
    in_file_path, 
    out_file_path, 
    out_tmp_folder,
    single_line_function, 
    buffer_segments, 
    sleep_duration=0
):

    def get_num_lines_of_file():
        print("counting lines of file.")
        result = subprocess.run(["wc", "-l", in_file_path], capture_output=True, text=True)
        num_lines = int(result.stdout.split()[0])
        print(f"input file has {num_lines} lines")
        return num_lines

    def get_segment_index_list(list_len, num_segment):
        segment_index_list = []
        step = list_len // num_segment
        i_start = 0
        for i_segment in range(1, num_segment + 1):
            if i_segment < num_segment:
                i_end = i_segment * step
                segment_index_list.append((i_start, i_end))
                i_start = i_end
            else:
                segment_index_list.append((i_start, list_len))
        return segment_index_list

    def single_process(p_id, i_start, i_end):
        print(f"process {p_id}: start")
        out_tmp_file_path = f"{out_tmp_folder}/{p_id}.txt"
        out_tmp_i_end_path = f"{out_tmp_folder}/{p_id}_i_end.txt"
        i_len = i_end - i_start
        i_buffer_list = get_segment_index_list(i_len, buffer_segments)
        i_buffer_set = set(s[1] + i_start for s in i_buffer_list)
        if os.path.exists(out_tmp_i_end_path):
            with open(out_tmp_i_end_path, "r") as f_i_end:
                i_start = int(f_i_end.read())
                print(f"process {p_id}: found previous start index: {i_start}")
        with open(in_file_path, "r") as f_in:
            with open(out_tmp_file_path, "a") as f_out:
                buffer_out_str = ""
                for i_line, line in enumerate(f_in):
                    if i_line >= i_start:
                        buffer_out_str += single_line_function(line)
                        if i_line != i_start and (i_line in i_buffer_set or i_line == i_end - 1):
                            f_out.write(buffer_out_str)
                            buffer_out_str = ""
                            i_line_rel = i_line - i_start
                            perc_current = round(100 / i_len * i_line_rel)
                            print(
                                f"process {p_id}: currently at {perc_current}% (line " 
                                f"{i_line}, until {i_end - 1})"
                            )
                            with open(out_tmp_i_end_path, "w") as f_i_end:
                                f_i_end.write(str(i_line))
                        if i_line == i_end - 1:
                            print(f"process {p_id}: done")
                            break

    def join_tmp_files():
        print("joining tmp files into one.")
        with open(out_file_path, "w") as f_out:
            for tmp_file_path in [out_tmp_folder + "/" + f for f in os.listdir(out_tmp_folder)]:
                if not tmp_file_path.endswith("_i_end.txt"):
                    print(f"writing content from {tmp_file_path}")
                    with open(tmp_file_path, "r") as f_in:
                        for line in f_in:
                            f_out.write(line)

    def multi_process_main():
        print(f"start multiprocessing, at {datetime.now()}")
        print("segmenting input file into slices to be assigned to cpu cores respectively.")
        if not os.path.exists(out_tmp_folder):
            os.mkdir(out_tmp_folder)
        segment_index_list = get_segment_index_list(get_num_lines_of_file(), cpu_cores)
        process_list = []
        for p_id, i_start_end_tuple in enumerate(segment_index_list):
            i_start, i_end = i_start_end_tuple[0], i_start_end_tuple[1]
            print(f"process {p_id}: assigned to indices from {i_start} to {i_end - 1}")
            process = Process(target=single_process, args=(p_id, i_start, i_end))
            process.start()
            # when dealing with big data objects, some processes are not instantiated; perhaps due
            # to race conditions between process invocation data memory handling. Adding a sleep 
            # here is a work-around to this issue.
            sleep(sleep_duration)
            process_list.append(process)
        for process in process_list:
            process.join()
        print(
            "Done with producing temporary file segments, merging them together into single output "
            "file."
        )
        join_tmp_files()
        print(f"done with multiprocessing, at {datetime.now()}")

    multi_process_main()


def main(main_func):

    def main_func_call(in_txt_file_path, out_txt_file_path, main_func):
        with open(in_txt_file_path, "r") as f_in:
            with open(out_txt_file_path, "w") as f_out:
                content_in = f_in.read()
                content_out = main_func(content_in)
                f_out.write(content_out)

    if IN_TXT_FILE and OUT_TXT_FILE:
        main_func_call(IN_TXT_FOLDER + IN_TXT_FILE, OUT_TXT_FOLDER + OUT_TXT_FILE, main_func)
    else:
        for file in os.listdir(IN_TXT_FOLDER):
            main_func_call(IN_TXT_FOLDER + file, OUT_TXT_FOLDER + file, main_func)
    if OUT_METADATA_FILE:
        write_veld_data_yaml()


import math
import os
import random
import subprocess
from datetime import datetime
from multiprocessing import Process


IN_FILE_PATH = "/veld/input/" + os.getenv("in_file")
OUT_FILE_PATH = "/veld/output/" + os.getenv("out_file")
OUT_LOG_PATH = "/veld/output/log.txt"
TMP_FILE_FOLDER = "/tmp"
SAMPLE_RANDOM_SEED = os.getenv("sample_random_seed")
PERCENTAGE_SAMPLE = float(os.getenv("percentage_sample"))
CPU_COUNT = os.getenv("cpu_count")
BUFFER_SEGMENTS = int(os.getenv("buffer_segments"))
if CPU_COUNT is None:
    CPU_COUNT = os.cpu_count()
else:
    CPU_COUNT = int(CPU_COUNT)
    if CPU_COUNT > os.cpu_count():
        CPU_COUNT = os.cpu_count()


def print_and_log(msg):
    print(msg, flush=True)
    with open(OUT_LOG_PATH, "a") as out:
        out.write(msg + "\n")


def get_line_indices():
    return list(range(10000))
    # print_and_log("counting lines")
    # line_indices_list = []
    # with open(IN_FILE_PATH, "r") as f_in:
    #     for i, line in enumerate(f_in):
    #         line_indices_list.append(i)
    #     print_and_log(f"total_line_count: {len(line_indices_list)}")
    # print_and_log("creating index list of random sample lines")
    # absolute_sample = int((len(line_indices_list) / 100) * PERCENTAGE_SAMPLE)
    # if SAMPLE_RANDOM_SEED is not None:
    #     random.seed(SAMPLE_RANDOM_SEED)
    # rand_indices = sorted(random.sample(line_indices_list, absolute_sample))
    # print_and_log(f"number of randomly sampled lines: {len(rand_indices)}")
    # return rand_indices


def single_process(p_id, individual_list):
    i_start = individual_list[0]
    rand_index_set = set(individual_list)
    out_tmp_file_path = f"{TMP_FILE_FOLDER}/{p_id}.txt"
    buffer_segment_step = math.ceil(len(rand_index_set) / BUFFER_SEGMENTS)
    with open(IN_FILE_PATH, "r") as f_in:
        with open(out_tmp_file_path, "w") as f_out:
            count_to_pick = len(rand_index_set)
            count_picked = 0
            buffer_out_str = ""
            for line_count, line in enumerate(f_in):
                if line_count >= i_start:
                    print(p_id)
                    break
                    # if line_count in rand_index_set:
                    #     count_picked += 1
                    #     buffer_out_str += line
                    #     rand_index_set.remove(line_count)
                    # if (
                    #     line_count != 0 
                    #     and (
                    #         line_count % buffer_segment_step == 0 
                    #         or count_picked == count_to_pick
                    #     )
                    # ):
                    #     f_out.write(buffer_out_str)
                    #     buffer_out_str = ""
                    #     print_and_log(
                    #         f"process {p_id}: picked {count_picked} lines, out of {count_to_pick}, "
                    #         f"currently at line {line_count}"
                    #     )
                    # if len(rand_index_set) == 0:
                    #     print_and_log(f"process {p_id}: done")
                    #     break


def multi_process(cpu_cores, global_list, single_process_function):

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

    def multi_process_main():
        segment_index_list = get_segment_index_list(len(global_list), cpu_cores)
        process_list = []
        for p_id, i_start_end_tuple in enumerate(segment_index_list):
            print_and_log(
                f"process id {p_id}: assigned to indices from {i_start_end_tuple[0]} to "
                f"{i_start_end_tuple[1] - 1}"
            )
            sub_list = global_list[i_start_end_tuple[0]:i_start_end_tuple[1]]
            process_list.append(Process(target=single_process_function, args=(p_id, sub_list)))
        for process in process_list:
            process.start()
        for process in process_list:
            process.join()

    multi_process_main()


def join_tmp_files():
    print_and_log("joining tmp files into one.")
    with open(OUT_FILE_PATH, "w") as f_out:
        for tmp_file_path in [TMP_FILE_FOLDER + "/" + f for f in os.listdir(TMP_FILE_FOLDER)]:
            with open(tmp_file_path, "r") as f_in:
                f_out.write(f_in.read())
    result = subprocess.run(["du", "-sh", OUT_FILE_PATH], capture_output=True, text=True)
    data_size = result.stdout.split()[0]
    # veld_data_yaml["x-veld"]["data"]["additional"]["data size"] = data_size
    print_and_log(f"done. Size of data: {data_size}")
    # with open(OUT_VELD_DATA_YAML_PATH, "w") as f:
    #     yaml.dump(veld_data_yaml, f, sort_keys=False)


def main():
    try:
        os.remove(OUT_LOG_PATH)
    except:
        pass
    print_and_log(f"starting at: {datetime.now()}")
    print_and_log(f"IN_FILE_PATH: {IN_FILE_PATH}")
    print_and_log(f"OUT_FILE_PATH: {OUT_FILE_PATH}")
    print_and_log(f"CPU_COUNT: {CPU_COUNT}")
    print_and_log(f"SAMPLE_RANDOM_SEED: {SAMPLE_RANDOM_SEED}")
    print_and_log(f"PERCENTAGE_SAMPLE: {PERCENTAGE_SAMPLE}")
    print_and_log(f"BUFFER_SEGMENTS: {BUFFER_SEGMENTS}")
    rand_indices_list = get_line_indices()
    multi_process(
        cpu_cores=CPU_COUNT, 
        global_list=rand_indices_list, 
        single_process_function=single_process
    )
    join_tmp_files()
    print_and_log(f"done at: {datetime.now()}")


if __name__ == "__main__":
    main()


from multiprocessing import Process
from time import sleep


def multi_process(cpu_cores, global_list, single_process_function, sleep_duration=0):

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
            print(
                f"process {p_id}: assigned to indices from {i_start_end_tuple[0]} to "
                f"{i_start_end_tuple[1] - 1}"
            )
            sub_list = global_list[i_start_end_tuple[0]:i_start_end_tuple[1]]
            process = Process(target=single_process_function, args=(p_id, sub_list))
            process.start()
            # when dealing with big data objects, some processes are not instantiated; perhaps due
            # to race conditions between process invocation data memory handling. Adding a sleep 
            # here is a work-around to this issue.
            sleep(sleep_duration)
            process_list.append(process)
        for process in process_list:
            process.join()

    multi_process_main()


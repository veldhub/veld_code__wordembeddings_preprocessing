import random
import os


INPUT_FILE_PATH = "/veld/input/" + os.getenv("in_file")
OUTPUT_FILE_PATH = "/veld/output/" + os.getenv("out_file")
PERCENTAGE_SAMPLE = float(os.getenv("percentage_sample"))


# print values
print(f"INPUT_FILE_PATH: {INPUT_FILE_PATH}")
print(f"OUTPUT_FILE_PATH: {OUTPUT_FILE_PATH}")
print(f"PERCENTAGE_SAMPLE: {PERCENTAGE_SAMPLE}")

# get total line count
with open(INPUT_FILE_PATH, "r") as f_in:
    total_line_count = 0
    for line in f_in:
        total_line_count += 1
    print(f"total_line_count: {total_line_count}")

# pick random sample indices
indices_all = list(range(total_line_count))
absolute_sample = int((total_line_count / 100) * PERCENTAGE_SAMPLE)
rand_indices = sorted(random.sample(indices_all, absolute_sample))
print(f"number of randomly sampled lines: {len(rand_indices)}")

# iterate and pick matching indices
with open(INPUT_FILE_PATH, "r") as f_in:
    with open(OUTPUT_FILE_PATH, "w") as f_out:
        for line_count, line in enumerate(f_in):
            if line_count == rand_indices[0]:
                del rand_indices[0]
                f_out.write(line)
                if rand_indices == []:
                    break


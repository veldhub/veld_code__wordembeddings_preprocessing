import os
import unicodedata

def is_letter(char):
    return unicodedata.category(char).startswith('L')



INPUT_FILE_PATH = "/veld/input/" + os.getenv("in_file")
OUTPUT_FILE_CLEAN_PATH = "/veld/output/" + os.getenv("out_file_clean")
OUTPUT_FILE_DIRTY_PATH = "/veld/output/" + os.getenv("out_file_dirty")
MIN_PERCENTAGE_CHAR = float(os.getenv("min_percentage_char"))
print(f"INPUT_FILE_PATH: {INPUT_FILE_PATH}")
print(f"OUTPUT_FILE_CLEAN_PATH: {OUTPUT_FILE_CLEAN_PATH}")
print(f"OUTPUT_FILE_DIRTY_PATH: {OUTPUT_FILE_DIRTY_PATH}")
print(f"MIN_PERCENTAGE_CHAR: {MIN_PERCENTAGE_CHAR}")

with open(INPUT_FILE_PATH, "r") as f_in:
    with open(OUTPUT_FILE_CLEAN_PATH, "w") as f_out_clean:
        with open(OUTPUT_FILE_DIRTY_PATH, "w") as f_out_dirty:
            count_total_clean = 0
            count_total_dirty = 0
            for line in f_in:
                count_clean = 0
                count_dirty = 0
                for char in line:
                    cat = unicodedata.category(char)
                    if cat.startswith("L") or cat.startswith("Z"):
                        count_clean += 1
                    else:
                        count_dirty += 1
                percentage_char = (100 * count_clean)  / (count_clean + count_dirty)
                if percentage_char >= MIN_PERCENTAGE_CHAR:
                    count_total_clean += 1
                    f_out_clean.write(line)
                else:
                    count_total_dirty += 1
                    f_out_dirty.write(line)

print(f"count_total_clean: {count_total_clean}")
print(f"count_total_dirty: {count_total_dirty}")
print(f"percentage clean: {(100 * count_total_clean) / (count_total_clean + count_total_dirty)}")
print(f"percentage dirty: {(100 * count_total_dirty) / (count_total_clean + count_total_dirty)}")


import os
import subprocess

import yaml


IN_TXT_PATH = "/veld/input/" + os.getenv("in_txt_file")
OUT_TXT_PATH = "/veld/output/" + os.getenv("out_txt_file")
OUT_DATA_DESCRIPTION = os.getenv("out_data_description")
OUT_VELD_DATA_YAML_PATH = "/veld/output/veld_data_lowercased.yaml"
print(f"IN_TXT_PATH: {IN_TXT_PATH}")
print(f"OUT_TXT_PATH: {OUT_TXT_PATH}")


veld_data_yaml = {
    "x-veld": {
        "data": {
            "description": OUT_DATA_DESCRIPTION,
            "topics": "NLP",
            "contents": [
                "training data",
                "raw text",
            ],
            "file_type": "json",
            "additional": {
                "data size": None,
            }
        }
    }
}


print("begin making all text lowercase.")
with open(IN_TXT_PATH, "r") as f_in:
    with open(OUT_TXT_PATH, "w") as f_out:
        for line in f_in:
            f_out.write(line.lower())
print("done")

result = subprocess.run(["du", "-sh", OUT_TXT_PATH], capture_output=True, text=True)
data_size = result.stdout.split()[0]
print(f"data size: {data_size}")
veld_data_yaml["x-veld"]["data"]["additional"]["data size"] = data_size
with open(OUT_VELD_DATA_YAML_PATH, "w") as f:
    yaml.dump(veld_data_yaml, f, sort_keys=False)


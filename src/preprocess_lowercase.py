import os
import subprocess

import yaml


IN_TXT_PATH = "/veld/input/" + os.getenv("in_txt_file")
OUT_TXT_PATH = "/veld/output/" + os.getenv("out_txt_file")
OUT_DATA_DESCRIPTION = os.getenv("out_data_description")
OUT_VELD_DATA_YAML_PATH = "/veld/output/veld_data_lowercased.yaml"


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


with open(IN_TXT_PATH, "r") as f_in:
    with open(OUT_TXT_PATH, "w") as f_out:
        for line in f_in:
            f_out.write(line.lower())

result = subprocess.run(["du", "-sh", OUT_TXT_PATH], capture_output=True, text=True)
data_size = result.stdout.split()[0]
veld_data_yaml["x-veld"]["data"]["additional"]["data size"] = data_size
with open(OUT_VELD_DATA_YAML_PATH, "w") as f:
    yaml.dump(veld_data_yaml, f, sort_keys=False)


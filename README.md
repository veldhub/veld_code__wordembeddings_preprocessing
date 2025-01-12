# veld_code__wordembeddings_preprocessing

This repo contains code velds relating to preprocessing for training wordembeddings.

The code velds may be integrated into chain velds or used stand-alone by modyfing the respective
veld yaml files directly

## requirements

- git
- docker compose

## code velds

This repo contains the following code velds. See inside their respective veld yaml files for more
information.

- [./veld_preprocess_clean.yaml](./veld_preprocess_clean.yaml) : Removes lines that don't reach a 
  threshold regarding the ratio of textual content to non-textual (numbers, special characters) 
  content.

- [./veld_preprocess_lowercase.yaml](./veld_preprocess_lowercase.yaml) : Makes entire text 
  lowercase.

- [./veld_preprocess_remove_punctuation.yaml](./veld_preprocess_remove_punctuation.yaml) : Removes 
  punctuation from text with spaCy pretrained models.

- [./veld_preprocess_sample.yaml](./veld_preprocess_sample.yaml) : Takes a random sample of lines 
  from a txt file. Randomness can be set with a seed too.

- [./veld_preprocess_strip.yaml](./veld_preprocess_strip.yaml) : Removes all lines before and after 
  given line numbers.


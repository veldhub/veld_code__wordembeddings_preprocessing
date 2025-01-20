# veld_code__wordembeddings_preprocessing

This repo contains [code velds](https://zenodo.org/records/13322913) encapsulating preprocessing 
for training of wordembeddings.

## requirements

- git
- docker compose (note: older docker compose versions require running `docker-compose` instead of 
  `docker compose`)

## how to use

A code veld may be integrated into a chain veld, or used directly by adapting the configuration 
within its yaml file and using the template folders provided in this repo. Open the respective veld 
yaml file for more information.

Run a veld with:
```
docker compose -f <VELD_NAME>.yaml up
```

## contained code velds

**[./veld_preprocess_clean.yaml](./veld_preprocess_clean.yaml)**

Removes lines that don't reach a threshold regarding the ratio of textual content to non-textual
(numbers, special characters) content.

```
docker compose -f veld_preprocess_clean.yaml up
```

**[./veld_preprocess_lowercase.yaml](./veld_preprocess_lowercase.yaml)**

Makes entire text lowercase.

```
docker compose -f veld_preprocess_lowercase.yaml up
```

**[./veld_preprocess_remove_punctuation.yaml](./veld_preprocess_remove_punctuation.yaml)**

Removes punctuation from text with spaCy pretrained models.

```
docker compose -f veld_preprocess_remove_punctuation.yaml up
```

**[./veld_preprocess_sample.yaml](./veld_preprocess_sample.yaml)**

Takes a random sample of lines from a txt file. Randomness can be set with a seed too.

```
docker compose -f veld_preprocess_sample.yaml up
```

**[./veld_preprocess_strip.yaml](./veld_preprocess_strip.yaml)**

Removes all lines before and after given line numbers.

```
docker compose -f veld_preprocess_strip.yaml up
```


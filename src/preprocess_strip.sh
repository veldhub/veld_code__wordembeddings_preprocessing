#!/bin/bash

in_file_path="/veld/input/${in_file}"
out_file_path="/veld/output/${out_file}"

if [[ $line_start != "" && $line_end != "" ]] ; then
  sed -n "${line_start},${line_end}p" $in_file_path > $out_file_path
elif [[ $line_start != "" && $line_end == "" ]] ; then
  sed -n "${line_start},\$p" $in_file_path > $out_file_path
elif [[ $line_start == "" && $line_end != "" ]] ; then
  sed -n "1,${line_end}p" $in_file_path > $out_file_path
elif [[ $line_start == "" && $line_end == "" ]] ; then
  echo "error: no line_start and line_end variables giving. Aborting."
fi

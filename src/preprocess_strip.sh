#!/bin/bash

if [[ $line_start != "" && $line_end != "" ]] ; then
  sed -n "${line_start},${line_end}p" $input_file_path > $output_file_path
elif [[ $line_start != "" && $line_end == "" ]] ; then
  sed -n "${line_start},\$p" $input_file_path > $output_file_path
elif [[ $line_start == "" && $line_end != "" ]] ; then
  sed -n "1,${line_end}p" $input_file_path > $output_file_path
elif [[ $line_start == "" && $line_end == "" ]] ; then
  echo "error: no line_start and line_end variables giving. Aborting."
fi

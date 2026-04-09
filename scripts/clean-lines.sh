#!/bin/bash

# Check if a directory path is provided as an argument
if [ -z "$1" ]; then
    echo "Please provide a path to the directory."
    exit 1
fi

cd "$1" || exit
pwd

sed -i 's/\t/ /g' *.txt
sed -i 's/![^ ]* //' *.txt
sed -i 's/  / /g' *.txt

sed -i ':a;N;$!ba;s/[[:space:]]*\n[[:space:]]*\(duration_of_algorithm\)/ \1/g' *.txt #join lines where duration_of_algorithm is at the beginning of the next line
sed -Ei 's/.*(algo_nr)/\1/' *.txt

sed -i 's/ input_size//g' *.txt
sed -i 's/ makespan_online//g' *.txt
sed -i 's/ makespan_offline//g' *.txt
sed -i 's/ makespan_heft//g' *.txt
sed -i 's/ duration_of_algorithm//g' *.txt
sed -i 's/ ms perceived//g' *.txt
sed -i 's/algo_nr //g' *.txt

find . -type f -exec sed -i 's/INVALID SWAP RATE: ON VERTEX[^.]*ratio\.//g' {} +
sed -i 's/ makespan_static//g' *.txt
sed -i 's/ makespan_dynamic//g' *.txt

sed -i '1i algo_nr wf_name inp_size dur_alg1 ms_1 dur_alg2 ms_2 ' *.txt
sed -i 's/  \+/ /g' *.txt #replace multiple spaces with a single space
sed -i '/^$/d' *.txt #remove empty lines
sed -i '/^\([[:space:]]*\([^[:space:]]\+\([[:space:]]\+[^[:space:]]\+\)\{0,2\}\)[[:space:]]*\)$/d' *.txt #remove lines with less than 3 columns (malformed, experiments that did not finish)


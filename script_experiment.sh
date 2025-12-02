#!/bin/bash
pwd

BINARY="./build/fonda_scheduler"
COMMON_ARGS="-m 1000000 -s 100 -r 1 -a heft-bl -f input/machines.csv -d 2 -S"

# Run experiments with different workloads
WORKLOADS=(
    "atacseq_200 2223941232"
    "atacseq_2000 2223941232"
    "atacseq_8000 2223941232"
    "chipseq_1000 4605965334"
    "chipseq_2000 4605965334"
    "chipseq_8000 4605965334"
    "eager_1000 14754556884"
    "eager_2000 14754556884"
    "eager_8000 14754556884"
    "methylseq_1000 11897958606"
    "methylseq_2000 11897958606"
    "methylseq_8000 11897958606"
)

for workload in "${WORKLOADS[@]}"; do
    IFS=' ' read -r -a params <<< "$workload"
    COMMAND="$BINARY ${COMMON_ARGS} -w ${params[0]} -i ${params[1]}"
    echo "Running command: $COMMAND"
    $COMMAND
done
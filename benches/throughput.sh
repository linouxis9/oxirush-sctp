#!/bin/sh
# Messages a second and CPU a message through SCTP associations over loopback, one message
# a call against batches, each case run ROUNDS times in turns: the median of each.
#
#   benches/throughput.sh                 # every case, 200 octets
#   OCTETS="100 400" benches/throughput.sh
#   CPUS=2,3 ROUNDS=5 benches/throughput.sh
#
# CPUS are the two CPUs that the program runs on (taskset), one for its sending task and
# one for its receiving one; the kernel's work for the loopback interface is counted in the
# CPU of the process. It prints a Markdown table, with the load of the machine.
set -eu
cd "$(dirname "$0")"
CPUS=${CPUS:-2,3}
ROUNDS=${ROUNDS:-3}
OCTETS=${OCTETS:-200}
cargo build --release --quiet --bin throughput
program=$(cargo metadata --format-version 1 --no-deps |
    sed 's/.*"target_directory":"\([^"]*\)".*/\1/')/release/throughput

# scenario associations messages batch
cases='pair 1 300000 1
pair 1 300000 1:64
pair 1 300000 64:1
pair 1 300000 64
to-endpoint 1 300000 1
to-endpoint 1 300000 1:64
to-endpoint 1 300000 64
to-endpoint 100 300000 1
to-endpoint 100 300000 1:64
to-endpoint 100 300000 64
to-endpoint 1000 300000 1
to-endpoint 1000 300000 1:64
to-endpoint 1000 300000 64
from-endpoint 1 300000 1
from-endpoint 1 300000 64:1
from-endpoint 1 300000 64
from-endpoint 100 100000 1
from-endpoint 100 100000 64'

results=$(mktemp)
trap 'rm -f "$results" "$results.case"' EXIT
before=$(cut -d' ' -f1-3 /proc/loadavg)
for octets in $OCTETS; do
    round=0
    while [ "$round" -lt "$ROUNDS" ]; do
        echo "$cases" | while read -r scenario associations messages batch; do
            line=$(taskset -c "$CPUS" "$program" "$scenario" "$octets" "$messages" \
                "$associations" "$batch")
            rate=$(echo "$line" | sed 's/.*: \([0-9]*\) messages\/s.*/\1/')
            cpu=$(echo "$line" | sed 's/.*s, \([0-9.]*\) us of CPU.*/\1/')
            echo "$octets $scenario $associations $batch $rate $cpu" >>"$results"
        done
        round=$((round + 1))
    done
done

echo "Load average $before before and $(cut -d' ' -f1-3 /proc/loadavg) after, $ROUNDS rounds, CPUs $CPUS."
echo
echo "| Octets | Scenario | Associations | Batch | Messages/s | CPU µs/message |"
echo "|---:|---|---:|---|---:|---:|"
# The median of each case, in the order of the cases
echo "$cases" | while read -r scenario associations messages batch; do
    for octets in $OCTETS; do
        grep "^$octets $scenario $associations $batch " "$results" >"$results.case" || true
        rate=$(cut -d' ' -f5 "$results.case" | sort -n | awk '{a[NR]=$1} END {print a[int((NR+1)/2)]}')
        cpu=$(cut -d' ' -f6 "$results.case" | sort -n | awk '{a[NR]=$1} END {print a[int((NR+1)/2)]}')
        echo "| $octets | $scenario | $associations | $batch | $rate | $cpu |"
    done
done

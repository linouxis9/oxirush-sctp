#!/bin/sh
# oxirush-sctp beside a program in C on the kernel's sockets (compare/sctp.c) and the SCTP
# crates of crates.io that run here, at the versions that compare/Cargo.toml pins, through
# the same loops over loopback (compare/src/main.rs says which).
#
#   benches/compare.sh                    # every case
#   CPUS=2,3 ROUNDS=5 benches/compare.sh
#
# Every case runs once a round, in turns, ROUNDS rounds, under `sudo -n perf stat` when it
# can, so that a busy moment of the machine falls on all of them; the run with the lowest
# time a message gives the row: its time, and its cycles and instructions, user and kernel,
# divided by its messages (the setup of a run is under a thousandth of them). Without perf
# they are "-". The allocations and their octets are counted by the allocator of the Rust
# program; the C program allocates nothing in its loops. The peak is the resident memory of
# the process.
# It prints a Markdown table, with the load of the machine.
set -eu
cd "$(dirname "$0")/compare"
CPUS=${CPUS:-2,3}
ROUNDS=${ROUNDS:-3}
cargo build --release --locked --quiet
target=$(cargo metadata --format-version 1 --no-deps |
    sed 's/.*"target_directory":"\([^"]*\)".*/\1/')
rust=$target/release/oxirush-sctp-compare
c=$target/release/sctp-c
cc -O2 -pthread -o "$c" sctp.c
perf=yes
sudo -n perf stat -e cycles true >/dev/null 2>&1 || perf=no

# case octets messages associations, then the implementations
cases='pingpong 200 200000 1 c oxirush-sctp oxirush-sctp-into tokio-sctp async-sctp lksctp sctp-rs
pingpong 16384 40000 1 c oxirush-sctp oxirush-sctp-into tokio-sctp async-sctp lksctp sctp-rs
stream 200 600000 1 c c-many oxirush-sctp oxirush-sctp-many tokio-sctp async-sctp lksctp sctp-rs
stream 200 600000 100 c c-many oxirush-sctp oxirush-sctp-many tokio-sctp async-sctp lksctp sctp-rs'

# The command of an implementation for a case of `messages`
program_of() { # implementation case octets messages associations
    case $1 in
    c) echo "$c $2 $3 $4 $5" ;;
    c-many) echo "$c $2-many $3 $4 $5" ;;
    *) echo "$rust $1 $2 $3 $4 $5" ;;
    esac
}

# Run a command, under perf when it can: the line that the program prints, then the cycles
# and the instructions of the run on one line. perf, as root, writes its file in the target
# directory: Linux refuses root a file of the user in a sticky /tmp.
measured() {
    counters=$(mktemp -p "$target")
    if [ "$perf" = yes ]; then
        # shellcheck disable=SC2086
        sudo -n perf stat -x, -o "$counters" -e cycles,instructions -- \
            taskset -c "$CPUS" $1 2>&1 | tail -1 || true
    else
        # shellcheck disable=SC2086
        taskset -c "$CPUS" $1 2>&1 | tail -1 || true
    fi
    run_cycles=$(grep ',cycles' "$counters" | cut -d, -f1)
    run_instructions=$(grep ',instructions' "$counters" | cut -d, -f1)
    rm -f "$counters"
    echo "${run_cycles:--} ${run_instructions:--}"
}

before=$(cut -d' ' -f1-3 /proc/loadavg)
rows=$(mktemp)
trap 'rm -f "$rows"' EXIT
round=0
while [ "$round" -lt "$ROUNDS" ]; do
    echo "$cases" | while read -r case octets messages associations implementations; do
        for implementation in $implementations; do
            key="$case|$octets|$associations|$implementation"
            output=$(measured "$(program_of "$implementation" "$case" "$octets" "$messages" "$associations")")
            line=$(echo "$output" | head -1)
            time=$(echo "$line" | sed -n 's/.*: \([0-9]*\) ns,.*/\1/p')
            if [ -z "$time" ]; then
                echo "$key|fails: $(echo "$line" | sed 's/.*fails: //' | cut -c1-60)" >>"$rows"
                continue
            fi
            set -- $(echo "$output" | tail -1)
            cycles=- instructions=-
            [ "$1" = - ] || cycles=$(($1 / messages))
            [ "$2" = - ] || instructions=$(($2 / messages))
            allocations=$(echo "$line" | sed 's/.* ns, \([0-9.]*\) allocations.*/\1/')
            allocated=$(echo "$line" | sed 's/.*allocations, \([0-9]*\) octets.*/\1/')
            peak=$(echo "$line" | sed 's/.*peak \([0-9]*\) KiB.*/\1/')
            echo "$key|$time|$cycles|$instructions|$allocations|$allocated|$peak" >>"$rows"
        done
    done
    round=$((round + 1))
done

echo "Load average $before before and $(cut -d' ' -f1-3 /proc/loadavg) after; CPUs $CPUS; the fastest of $ROUNDS runs in turns."
echo
echo "| Case | Implementation | ns a message | Cycles | Instructions | Allocations | Octets allocated | Peak memory | Time ÷ oxirush-sctp |"
echo "|---|---|---:|---:|---:|---:|---:|---:|---:|"
# The fastest run of each case, in the order of the cases; a case that never ran fails
awk -F'|' '
    {
        key = $1 "|" $2 "|" $3 "|" $4
        if (!(key in seen)) { seen[key] = 1; order[++n] = key }
        if ($5 ~ /fails/) { if (!(key in best)) failed[key] = $5; next }
        if (!(key in best) || $5 + 0 < time[key] + 0) { best[key] = $0; time[key] = $5 }
        if ($4 == "oxirush-sctp" && (!(($1 "|" $2 "|" $3) in ours) || $5 + 0 < ours[$1 "|" $2 "|" $3] + 0)) ours[$1 "|" $2 "|" $3] = $5
    }
    END {
        for (i = 1; i <= n; i++) {
            key = order[i]
            split(key, k, "|")
            name = (k[1] == "pingpong") ? "ping-pong, " k[2] " octets" : k[2] " octets, " k[3] (k[3] == 1 ? " association" : " associations")
            if (!(key in best)) { printf "| %s | %s | %s | | | | | | |\n", name, k[4], failed[key]; continue }
            split(best[key], f, "|")
            case_key = k[1] "|" k[2] "|" k[3]
            ratio = (case_key in ours) ? sprintf("%.2f", f[5] / ours[case_key]) : "-"
            printf "| %s | %s | %s | %s | %s | %s | %s | %.1f MiB | %s |\n", name, k[4], f[5], f[6], f[7], f[8], f[9], f[10] / 1024, ratio
        }
    }' "$rows"

#!/usr/bin/env bash
# Run each benchmark config in isolation to avoid thermal/memory effects

set -e

OUTPUT_FILE=".claude-scratch/bench_isolated_$(date +%Y%m%d_%H%M%S).txt"
mkdir -p .claude-scratch

echo "Running isolated benchmarks..."
echo "Output: $OUTPUT_FILE"
echo ""

run_bench() {
    local filter="$1"
    # thrpt line: "thrpt:  [1.08 GiB/s 1.09 GiB/s 1.10 GiB/s]"
    # extract median (middle value): fields 4-5
    cargo bench --bench transform_to_parquet -- "$filter" 2>&1 \
        | grep -E '^\s+thrpt:' \
        | head -1 \
        | awk '{print $4, $5}'
}

# header
printf "%-20s %14s %14s %14s\n" "Config" "Sequential" "Stream" "Eager" | tee "$OUTPUT_FILE"
printf "%-20s %14s %14s %14s\n" "------" "----------" "------" "-----" | tee -a "$OUTPUT_FILE"

for config in 5cols_10Mrows 9cols_10Mrows 17cols_10Mrows 5cols_100Mrows 9cols_100Mrows 17cols_100Mrows; do
    echo "  $config..." >&2

    seq_thrpt=$(run_bench "seq/$config")
    sleep 2

    stream_thrpt=$(run_bench "stream/$config")
    sleep 2

    eager_thrpt=$(run_bench "eager/$config")
    sleep 2

    printf "%-20s %14s %14s %14s\n" "$config" "$seq_thrpt" "$stream_thrpt" "$eager_thrpt" | tee -a "$OUTPUT_FILE"
done

echo ""
echo "Done! Results in $OUTPUT_FILE"

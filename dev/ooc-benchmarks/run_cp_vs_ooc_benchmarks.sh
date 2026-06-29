#!/usr/bin/env bash
set -euo pipefail

SYSDS_CMD="${SYSDS_CMD:-./bin/systemds}"

echo "CP vs OOC benchmark runner"
echo "SystemDS command: ${SYSDS_CMD}"
echo

run_case() {
  local name="$1"
  local mode="$2"
  shift 2

  echo "============================================================"
  echo "CASE: ${name}"
  echo "MODE: ${mode}"
  echo "COMMAND: $*"
  echo "============================================================"

  if [ "${mode}" = "OOC" ]; then
    /usr/bin/time -p "${SYSDS_CMD}" "$1" -ooc -args "${@:2}"
  else
    /usr/bin/time -p "${SYSDS_CMD}" "$1" -args "${@:2}"
  fi

  echo
}

COV_SCRIPT="dev/ooc-benchmarks/ooc_covariance_benchmark.dml"
TSMM_SCRIPT="dev/ooc-benchmarks/ooc_tsmm_benchmark.dml"

echo "Warm-up runs"
run_case "cov(A,B) warmup" "CP" "${COV_SCRIPT}" 10000 0.8 0
run_case "cov(A,B) warmup" "OOC" "${COV_SCRIPT}" 10000 0.8 0

echo "Measured covariance runs"
run_case "cov(A,B) dense" "CP" "${COV_SCRIPT}" 50000 0.8 0
run_case "cov(A,B) dense" "OOC" "${COV_SCRIPT}" 50000 0.8 0

run_case "cov(A,B) sparse" "CP" "${COV_SCRIPT}" 50000 0.1 0
run_case "cov(A,B) sparse" "OOC" "${COV_SCRIPT}" 50000 0.1 0

run_case "cov(A,B,W) dense" "CP" "${COV_SCRIPT}" 50000 0.8 1
run_case "cov(A,B,W) dense" "OOC" "${COV_SCRIPT}" 50000 0.8 1

run_case "cov(A,B,W) sparse" "CP" "${COV_SCRIPT}" 50000 0.1 1
run_case "cov(A,B,W) sparse" "OOC" "${COV_SCRIPT}" 50000 0.1 1

echo "Measured TSMM runs"
run_case "t(X)%*%X dense" "CP" "${TSMM_SCRIPT}" 5000 1000 0.8 0
run_case "t(X)%*%X dense" "OOC" "${TSMM_SCRIPT}" 5000 1000 0.8 0

run_case "t(X)%*%X sparse" "CP" "${TSMM_SCRIPT}" 5000 1000 0.1 0
run_case "t(X)%*%X sparse" "OOC" "${TSMM_SCRIPT}" 5000 1000 0.1 0

run_case "X%*%t(X) dense" "CP" "${TSMM_SCRIPT}" 2000 500 0.8 1
run_case "X%*%t(X) dense" "OOC" "${TSMM_SCRIPT}" 2000 500 0.8 1

run_case "X%*%t(X) sparse" "CP" "${TSMM_SCRIPT}" 2000 500 0.1 1
run_case "X%*%t(X) sparse" "OOC" "${TSMM_SCRIPT}" 2000 500 0.1 1

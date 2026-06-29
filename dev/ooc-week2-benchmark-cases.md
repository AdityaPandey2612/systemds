# Week 2 OOC Benchmark Cases


## Scope

Local CP vs local OOC only. Spark and cluster benchmarking are out of scope.

## Week 2 Focus

- cov(A, B)
- cov(A, B, W)

## Planned Cases

| Case | Rows | Cols | Sparsity | Block size | Runs |
|---|---:|---:|---:|---:|---:|
| unweighted dense small | 10000 | 1 | 0.9 | 1000 | 1 warm-up + 3 measured |
| unweighted sparse small | 10000 | 1 | 0.1 | 1000 | 1 warm-up + 3 measured |
| weighted dense small | 10000 | 1 | 0.9 | 1000 | 1 warm-up + 3 measured |
| weighted sparse small | 10000 | 1 | 0.1 | 1000 | 1 warm-up + 3 measured |
| unweighted dense larger | 100000 | 1 | 0.9 | 1000 | 1 warm-up + 3 measured |
| weighted dense larger | 100000 | 1 | 0.9 | 1000 | 1 warm-up + 3 measured |

## Metrics

- operator
- execution mode: CP or OOC
- rows
- columns
- sparsity
- block size
- runtime in seconds
- correctness compared against CP
- notes or failure reason

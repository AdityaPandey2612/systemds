# Week 4 Final Results


## Target Branch

The MR target branch is:

`aditya-amls-test-branch`

## Final Correctness Tests

The following focused OOC tests were run:

```bash

mvn -Dtest=org.apache.sysds.test.functions.ooc.CovarianceTest test

mvn -Dtest=org.apache.sysds.test.functions.ooc.CovarianceWeightsTest test

mvn -Dtest=org.apache.sysds.test.functions.ooc.TransposeSelfMMTest test
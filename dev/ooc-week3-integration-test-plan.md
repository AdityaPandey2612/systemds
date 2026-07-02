# Week 3: OOC Integration Test Plan

## Goal

The goal of Week 3 is to validate the OOC covariance and TSMM work after the implementation branches are merged into the target branch.

## Target Branch

The MR target branch is:

`aditya-amls-test-branch`

## Covariance Tests

Run:

```bash

mvn -Dtest=org.apache.sysds.test.functions.ooc.CovarianceTest test

mvn -Dtest=org.apache.sysds.test.functions.ooc.CovarianceWeightsTest test

mvn -Dtest=org.apache.sysds.test.functions.ooc.TransposeSelfMMTest test

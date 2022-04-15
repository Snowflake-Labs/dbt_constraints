#!/bin/bash

dbt clean
dbt deps
dbt compile
dbt docs generate
cp ./target/*.json ./docs/
cp ./target/*.html ./docs/

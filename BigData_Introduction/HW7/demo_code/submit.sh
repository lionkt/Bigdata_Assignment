#!/usr/bin/env bash
spark-submit --name test --verbose --master yarn-cluster --executor-memory 1g --num-executors 1 --executor-cores 1 SimpleApp.py

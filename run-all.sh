#!/bin/sh
# This is a comment!

echo 'Enter path for spark bin folder: Example: D:/Software/spark-2.3.2-bin-hadoop2.7/bin'

read SPARK_SUBMIT_PATH
#SPARK_SUBMIT_PATH=$1
SPARK_SUBMIT_PATH+='/spark-submit.cmd'

echo Starting implementation

echo Starting task1
task1_val= $($SPARK_SUBMIT_PATH Mansi_Ganatra_task1.py review.json task1_result.json > task1_log.txt)
echo Completed task1

echo Starting task2
task2_val= $($SPARK_SUBMIT_PATH Mansi_Ganatra_task2.py review.json task2_result.json 40 > task2_log.txt)
echo Completed task2

echo Starting task3
task3_val= $($SPARK_SUBMIT_PATH Mansi_Ganatra_task3.py review.json business.json task3_output.txt task3_result.json > task3_log.txt)
echo Completed task3
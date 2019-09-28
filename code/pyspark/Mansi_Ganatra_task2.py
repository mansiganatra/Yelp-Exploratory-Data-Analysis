from pyspark import SparkContext, StorageLevel
import time
import sys
import json

if len(sys.argv) != 4:
    print("Usage: ./bin/spark-submit task2.py <input_file_path> <output_file_path> n_partition")
    exit(-1)
else:
    input_file_path = sys.argv[1]
    output_file_path = sys.argv[2]
    n_partition = int(sys.argv[3])

result = {}
SparkContext.setSystemProperty('spark.executor.memory', '4g')
SparkContext.setSystemProperty('spark.driver.memory', '4g')
sc = SparkContext('local[*]', 'task2')

# input_file_path = "D:/Sem2/INF553/Assignments/1/yelp_dataset/review.json"
# output_file_path = "D:/Sem2/INF553/Assignments/1/yelp_dataset/task2_result.json"
# n_partition = 40

reviewsRDD = sc.textFile(input_file_path).map(lambda x: json.loads(x))\
    .map(lambda entry: (entry['business_id'], 1))\
    # .persist(storageLevel=StorageLevel.MEMORY_ONLY)

#Q6. The top 10 businesses that had the largest number of reviews and the number of those reviews
default_start_time = time.time()

# top10_businesses = reviewsRDD\
#     .reduceByKey(lambda a, b: a+b)\
#     .map(lambda entry: (entry[1], entry[0]))\
#     .sortByKey(ascending=False)\
#     .map(lambda entry: (entry[1], entry[0]))\
#     .take(10)

top10_businesses = reviewsRDD\
    .reduceByKey(lambda a, b: a+b)\
    .sortBy(lambda entry: (-entry[1], entry[0]))\
    .take(10)

#Q2a
default_end_time = time.time()

default_total_time = default_end_time - default_start_time

default_num_of_partitions = reviewsRDD.getNumPartitions()
default_partition_data = reviewsRDD \
    .glom() \
    .map(lambda partition: len(partition)) \
    .collect()

# print("no. of partitions:  " + str(default_num_of_partitions))
# print("time taken: " + str(default_total_time))

default_values = {}
default_values['n_partition'] = default_num_of_partitions
default_values['n_items'] = default_partition_data
default_values['exe_time'] = default_total_time
result['default'] = default_values

# Q2b
#***************************** customize partition function **********************************

new_reviewsRDD = reviewsRDD.partitionBy(n_partition, lambda entry: hash(entry[0]))

revised_start_time = time.time()

# top10_businesses_new = new_reviewsRDD\
#     .reduceByKey(lambda a, b: a+b)\
#     .map(lambda entry: (entry[1], entry[0]))\
#     .sortByKey(ascending=False)\
#     .map(lambda entry: (entry[1], entry[0]))\
#     .take(10)


top10_businesses_new = new_reviewsRDD\
    .reduceByKey(lambda a, b: a+b)\
    .sortBy(lambda entry: (-entry[1], entry[0]))\
    .take(10)

revised_end_time = time.time()
revised_total_time = revised_end_time - revised_start_time

revised_num_of_partitions = new_reviewsRDD.getNumPartitions()

revised_partition_data = new_reviewsRDD \
    .glom()\
    .map(lambda partition: len(partition))\
    .collect()

revised_values = {}
revised_values['n_partition'] = revised_num_of_partitions
revised_values['n_items'] = revised_partition_data
revised_values['exe_time'] = revised_total_time
result['customized'] = revised_values

explanation = "After using customized partition function to " + str(revised_num_of_partitions) \
              + " the computation time decreased. Using the hash of business_id field, " \
                "upon which the query is based, for the customized partition function, makes the computation faster."
result['explanation'] = explanation

with open(output_file_path, "w+") as fp:
    json.dump(result, fp)

from pyspark import SparkContext
from time import time
import sys
import json

if len(sys.argv) != 5:
    print("Usage: ./bin/spark-submit task3.py <input_file_path_1> <input_file_path_2> <output_file_path_1> <output_file_path_2>")
    exit(-1)
else:
    reviews_input_file_path = sys.argv[1]
    business_input_file_path = sys.argv[2]
    content_output_file_path = sys.argv[3]
    result_output_file_path = sys.argv[4]

result = {}
SparkContext.setSystemProperty('spark.executor.memory', '4g')
SparkContext.setSystemProperty('spark.driver.memory', '4g')
sc = SparkContext('local[*]', 'task3')

# reviews_input_file_path = "D:/Sem2/INF553/Assignments/1/yelp_dataset/review.json"
# business_input_file_path = "D:/Sem2/INF553/Assignments/1/yelp_dataset/business.json"
# content_output_file_path = "D:/Sem2/INF553/Assignments/1/yelp_dataset/task3_output.csv"
# result_output_file_path = "D:/Sem2/INF553/Assignments/1/yelp_dataset/task3_result.json"

reviewsRDD = sc.textFile(reviews_input_file_path).map(lambda x: json.loads(x)).map(lambda entry: (entry['business_id'], entry['stars']))
businessRDD = sc.textFile(business_input_file_path).map(lambda x: json.loads(x)).map(lambda entry: (entry['business_id'], entry['city']))
# print(type(reviewsRDD))
# print(type(businessRDD))

#Q1a. What is the avaervage stars for each city
finalRdd = reviewsRDD.join(businessRDD)\
    .map(lambda key_value: (key_value[1][1], key_value[1][0]))\
    .mapValues(lambda value: (value,1))\
    .reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))\
    .mapValues(lambda value: value[0]/value[1])\
    .sortBy(lambda entry: (-entry[1], entry[0]))\
    # .persist(storageLevel=StorageLevel.MEMORY_ONLY)

# print(finalRdd)
# print(datetime.now())

#Q2b. Two ways to print first 10 cities

# Part 1 Collect all data and print first 10

collect_start_time = time()
top10_cities_collect = finalRdd.collect()
print(top10_cities_collect[:10])
collect_end_time = time()
collect_total_time = collect_end_time - collect_start_time
print(collect_total_time)

result['m1'] = collect_total_time

with open(content_output_file_path, 'w+', encoding="utf-8") as fp:
    fp.write("city,stars")
    fp.write('\n')
    fp.write('\n'.join('{},{}'.format(x[0],x[1]) for x in top10_cities_collect))

# Part 2 Take first 10 cities and print them

take_start_time = time()
top10_cities_take = finalRdd.take(10)
print(top10_cities_take)
take_end_time = time()
take_total_time = take_end_time - take_start_time
print(take_total_time)

result['m2'] = take_total_time
result['explanation'] = ".collect() first computes all the transformations and returns all the values in the RDD, " \
                        "while .take(n) computes the transformations only until n values are found. " \
                        "Hence .take(n) works faster than .collect()"

with open(result_output_file_path, 'w+') as rfp:
    json.dump(result, rfp)
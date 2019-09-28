import java.io.PrintWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.json4s.{DefaultFormats, JObject, NoTypeHints}
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

object Mansi_Ganatra_task3 {

  def main(args: Array[String]): Unit = {

    if(args.length != 4 ) {
      println("Usage: ./bin/spark-submit Mansi_Ganatra_task3.class Mansi_Ganatra_hw1.jar <input_file_path_1> <input_file_path_2> <output_file_path_1> <output_file_path_2>")
      return
    }

    val input_file_path_review  = args(0)
    val input_file_path_business  = args(1)
    val output_file_path_content = args(2)
    val output_file_path = args(3)

    Logger.getLogger("org").setLevel(Level.WARN)

    val ss = SparkSession
      .builder()
      .appName("task3")
      .config("spark.master", "local[*]")
      .getOrCreate()
    val sc = ss.sparkContext

//    val input_file_path_reviews = "D:/Sem2/INF553/Assignments/1/yelp_dataset/review.json"
//    val input_file_path_business = "D:/Sem2/INF553/Assignments/1/yelp_dataset/business.json"
//    val output_file_path = "D:/Sem2/INF553/Assignments/1/yelp_dataset/task3_result_scala.json"
//    val output_file_path_content = "D:/Sem2/INF553/Assignments/1/yelp_dataset/task3_output_scala.txt"

    val reviewsRdd = sc.textFile(input_file_path_review)
      .map(jsonEntry => {
        implicit val jsonFormat = DefaultFormats
        val parsedReview = parse(jsonEntry).asInstanceOf[JObject]
        val businessId = (parsedReview \ "business_id").extract[String]
        val stars = (parsedReview \ "stars").extract[Double]
        (businessId, stars)
      })
      .persist(StorageLevel.MEMORY_ONLY)

//    println(reviewsRdd.toString)

    val businessRdd = sc.textFile(input_file_path_business)
      .map(jsonEntry => {
        implicit val jsonFormat = DefaultFormats
        val parsedReview = parse(jsonEntry).asInstanceOf[JObject]
        val businessId = (parsedReview \ "business_id").extract[String]
        val city = (parsedReview \ "city").extract[String]
        (businessId, city)
      })
      .persist(StorageLevel.MEMORY_ONLY)


    val finalRdd = reviewsRdd.join(businessRdd)
      .map(finalEntry => (finalEntry._2._2, (finalEntry._2._1, 1)))
      .reduceByKey((v1, v2) => (v1._1+v2._1, v1._2+v2._2))
      .mapValues(value => value._1/value._2)
      .sortBy(finalEntry => (-finalEntry._2, finalEntry._1))


//    Q2b. Two ways to print first 10 cities

//     Part 1 Collect all data and print first 10

    val collect_start_time = System.currentTimeMillis()
    val top10_cities_collect = finalRdd.collect()
    top10_cities_collect.take(10).foreach(println)
    val collect_end_time = System.currentTimeMillis()

    val collect_total_time = (collect_end_time - collect_start_time).toDouble/1000
//    println(collect_total_time)

//    Part 2 Take first 10 cities and print them

    val take_start_time = System.currentTimeMillis()
    val top10_cities_take = finalRdd.take(10)
    top10_cities_take.foreach(println)
    val take_end_time = System.currentTimeMillis()
    val take_total_time = (take_end_time - take_start_time).toDouble/1000
//    print(take_total_time)

    val result = scala.collection.mutable.Map[String, Any]()
    val explanation = ".collect() first computes all the transformations and returns all the values in the RDD, " +
      "while .take(n) computes the transformations only until n values are found. Hence .take(n) works faster than .collect()"

    result += "m1" -> collect_total_time
    result += "m2" -> take_total_time
    result += "explanation" -> explanation

    implicit val formats = Serialization.formats(NoTypeHints)

    val output = new PrintWriter(output_file_path)
    val output_string = write(result)
    output.write(output_string)
    output.close()

    val output_content = new PrintWriter(output_file_path_content)
    val output_content_string = top10_cities_collect.map(_.productIterator.mkString(",")).mkString("\n")
    output_content.write("city,stars")
    output_content.write("\n")
    output_content.write(output_content_string)
    output_content.close()
  }
}

import java.io.PrintWriter

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization._

import scala.util.parsing.json.JSON
object Mansi_Ganatra_task1 {
  def main(args: Array[String]): Unit = {

    if(args.length != 2) {
      println("Usage: ./bin/spark-submit Mansi_Ganatra_task1.class Mansi_Ganatra_hw1.jar <input_file_path> <output_file_path>")
      return
    }
    val input_file_path = args(0)
    val output_file_path =args(1)

    Logger.getLogger("org").setLevel(Level.INFO)

    val ss = SparkSession
      .builder()
      .appName("task1")
      .config("spark.master", "local[*]")
      .getOrCreate()
    val sc = ss.sparkContext

//    val input_file_path = "D:/Sem2/INF553/Assignments/1/yelp_dataset/review.json"
//    val output_file_path = "D:/Sem2/INF553/Assignments/1/yelp_dataset/task1_result_scala.json"

    val result = scala.collection.mutable.Map[String, Any]()

    val reviewsRdd = sc.textFile(input_file_path)
      .map(jsonEntry => {
        implicit val jsonFormat = DefaultFormats
        val parsedReview = parse(jsonEntry).asInstanceOf[JObject]
        val reviewId = (parsedReview \ "review_id").extract[String]
        val businessId = (parsedReview \ "business_id").extract[String]
        val userId = (parsedReview \ "user_id").extract[String]
        val date = (parsedReview \ "date").extract[String]
        ((reviewId, businessId, userId, date), 1)
      })
      .persist(StorageLevel.MEMORY_ONLY)


//    Q1. count total number of reviews

    val total_count = reviewsRdd.map(entry => {
      val reviewId = entry._1._1
      (reviewId,1)
    }).distinct().count()
    result += "n_review" -> total_count


//    Q2. count total reviews in 2018

    val count_2018 = reviewsRdd
      .filter(entry => entry._1._4.contains("2018"))
      .count()
    result += "n_review_2018" -> count_2018

//    Q3. The number of distinct users who wrote reviews

    val distinct_users = reviewsRdd.map(entry => {
      val userId = entry._1._3
      (userId,1)
    }).distinct().count()
    result += "n_user" -> distinct_users

//    Q4. Top 10 users who wrote largest number of reviews and the number of reviews they wrote

//    val top10_users = reviewsRdd
//      .map(entry => {
//      val userId = entry._1._3
//      (userId,1)
//    })
//      .reduceByKey(_+_)
//      .map( entry => (entry._2, entry._1))
//      .sortByKey(ascending = false)
//      .map(entry => (entry._2, entry._1))
//      .take(10)

    val top10_users = reviewsRdd
      .map(entry => {
        val userId = entry._1._3
        (userId,1)
      })
      .reduceByKey(_+_)
      .sortBy(entry => (-entry._2, entry._1))
      .take(10)
      .map(tuple => Array(tuple._1, tuple._2))


    result += "top10_user" -> top10_users.toList

//    Q5. The distinct number of businesses that have been reviewes

    val distinct_businesses = reviewsRdd
      .map(entry => {
      val businessId = entry._1._2
      (businessId,1)
    }).distinct().count()
    result += "n_business" -> distinct_businesses

//    Q6. The top 10 businesses that had the largest number of reviews and the number of those reviews

//    val top10_businesses =  reviewsRdd
//      .map(entry => {
//        val businessId = entry._1._2
//        (businessId,1)
//      })
//      .reduceByKey(_+_)
//      .map( entry => (entry._2, entry._1))
//      .sortByKey(ascending = false)
//      .map(entry => (entry._2, entry._1))
//      .take(10)

    val top10_businesses =  reviewsRdd
      .map(entry => {
        val businessId = entry._1._2
        (businessId,1)
      })
      .reduceByKey(_+_)
      .sortBy(entry => (-entry._2, entry._1))
      .take(10)
      .map(tuple => Array(tuple._1, tuple._2))

    result += "top10_business" -> top10_businesses


//     Writ results to file
//    with open(output_file_path, 'w+') as out:
//      json.dump(result, out)
    implicit val formats = Serialization.formats(NoTypeHints)

    val output = new PrintWriter(output_file_path)
    val output_string = write(result)
    println(output_string)
    output.write(output_string)

    output.close()
//    println(total_count)
//    println(count_2018)
//    println(distinct_users)
//    println(distinct_businesses)
//    top10_users.foreach(p => {
//      println(p._1)
//      print(p._2)
//    })
//    top10_businesses.foreach(p => {
//      println(p._1)
//      print(p._2)
//    })
//
//    println(write(result))
  }
}

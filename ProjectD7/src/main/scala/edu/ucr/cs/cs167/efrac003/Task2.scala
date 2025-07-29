package edu.ucr.cs.cs167.efrac003

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Task2 {
  def main(args: Array[String]): Unit = {
    val inputFile: String = args(0)

    val conf = new SparkConf().setAppName("CS167 Project D")
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    println(s"Using Spark master '${conf.get("spark.master")}'")

    val spark = SparkSession.builder().config(conf)
    val sparkSession = spark.getOrCreate()

    val cleanTweets = sparkSession.read.format("json")
      .option("sep", "\t")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(inputFile)
    cleanTweets.createOrReplaceTempView("cleanTweets")

    val top20Hashtags = sparkSession.sql("""
      SELECT hashtags FROM (
        SELECT explode(hashtags) AS hashtags, COUNT(*) AS count
        FROM cleanTweets
        GROUP BY hashtags
        ORDER BY count DESC
        LIMIT 20
      )
    """).collect().map(_.getString(0)).toList

    val tweetsWithTopics = sparkSession.sql(s"""
      SELECT
        id,
        text,
        created_at,
        country_code,
        COALESCE(element_at(array_intersect(hashtags, ARRAY(${top20Hashtags.map(h => s"'$h'").mkString(", ")})), 1), '') AS topic,
        user_description,
        retweet_count,
        reply_count,
        quoted_status_id
      FROM cleanTweets
      WHERE size(array_intersect(hashtags, ARRAY(${top20Hashtags.map(h => s"'$h'").mkString(", ")}))) > 0
    """)

    tweetsWithTopics.write.json("tweets_topic.json")

    sparkSession.stop()
  }
}

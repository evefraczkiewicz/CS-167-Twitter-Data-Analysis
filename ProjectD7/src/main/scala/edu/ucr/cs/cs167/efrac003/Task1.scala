//package edu.ucr.cs.cs167.fcuar002
package edu.ucr.cs.cs167.efrac003

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Task1 {
  
  def main(args : Array[String]) {
    if (args.length != 1) {
      println("Usage <input file>")
      println("  - <input file> path to a JSON file input")
      sys.exit(0)
    }
    val inputfile: String = args(0)

    val conf = new SparkConf
    if (!conf.contains("spark.master"))
      conf.setMaster("local[*]")
    println(s"Using Spark master '${conf.get("spark.master")}'")

    val spark = SparkSession
      .builder()
      .appName("CS167 Project")
      .config(conf)
      .getOrCreate()

    try {
      val data: DataFrame = spark.read.format("json")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(inputfile)

      data.createOrReplaceTempView("tweets")
      //id, text, created_at, place.country_code, entities.hashtags.text AS hashtags, user.description as user_description, retweet_count, reply_count, quoted_status_id
      val mainquery: String = "SELECT id, text, created_at, place.country_code, entities.hashtags.text AS hashtags, user.description as user_description, retweet_count, reply_count, quoted_status_id FROM tweets;"
      val mainatts = spark.sql(mainquery)
      mainatts.write.json("tweets_clean.json")

      val expldquery: String = "SELECT explode(entities.hashtags.text) FROM tweets;"
      val exploded = spark.sql(expldquery)
      exploded.createOrReplaceTempView("explded")
      val hashtagsquery: String = "SELECT col, COUNT(*) AS count FROM explded GROUP BY col ORDER BY count DESC LIMIT 20;"
      val hashtags = spark.sql(hashtagsquery)

      val keywords = hashtags.select("col").collect()
      keywords.foreach(println)
      println(keywords)

    } finally {
      spark.stop
    }

  }

}

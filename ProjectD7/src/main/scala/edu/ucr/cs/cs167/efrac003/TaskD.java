//package edu.ucr.cs.cs167.nnati004;
package edu.ucr.cs.cs167.efrac003;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;

public class TaskD {
    public static void main(String[] args) {
        String startDate = args[0]; // format: MM/DD/YY
        String endDate = args[1];   // ex. 03/15/2018
        String inputFile = args[2]; // tweets1k_clean.json
        SparkConf conf = new SparkConf();
        if (!conf.contains("spark.master"))
            conf.setMaster("local[*");

        System.out.println("Using Spark master '" + conf.get("spark.master") + "'");

        SparkSession spark = SparkSession.builder().appName("CS167_Project").config("spark.sql.legacy.timeParserPolicy", "LEGACY").config(conf).getOrCreate();
        //Load the dataset named tweets_clean from the Task 1 in the Json format.
        DataFrameReader reader = spark.read();
        Dataset<Row> df;
        if(inputFile.endsWith(".json")){
            df = reader.json(inputFile);
        } else {
            System.err.println("Unexpected input format. Expected file name to end with either '.json'");
            return;
        }
        df.createOrReplaceTempView("tweets");

        try{
            //Parse the created_at attribute into a proper timestamp attribute. For that use the SQL function to_timestamp with the format EEE MMM dd HH:mm:ss Z yyyy.
            df = spark.sql("SELECT *, to_timestamp(created_at, 'EEE MMM dd HH:mm:ss Z yyyy') AS parsed_created_at FROM tweets");

            // Parse the user-provided start and end dates
            Dataset<Row> dateRange = spark.sql("SELECT to_date('" + startDate + "', 'MM/dd/yyyy') AS start_date, " +
                    "to_date('" + endDate + "', 'MM/dd/yyyy') AS end_date");

            // Include a WHERE clause to filter tweets between start and end dates
            df.createOrReplaceTempView("tweets_with_parsed_dates");
            dateRange.createOrReplaceTempView("date_range");
            dateRange.show();
            //Include a WHERE clause that tests if the tweets are is BETWEEN start AND end dates.
            //Include a grouped aggregate statement to count the number of tweets for each country_code.
            //Show only those country_codes and correspoding count values in the result whose count value is greater than 50.
            df = spark.sql("SELECT t.country_code, COUNT(*) AS tweet_count " +
                    "FROM tweets_with_parsed_dates t, date_range d " +
                    "WHERE t.parsed_created_at BETWEEN d.start_date AND d.end_date " +
                    "AND t.country_code IS NOT NULL " +
                    "GROUP BY t.country_code " +
                    "HAVING tweet_count >= 50 ");
            df.show();

            String outputPath = "CountryTweetsCount.csv";
            df.coalesce(1) // Ensure that the DataFrame produces a single output file
                    .write()
                    .option("header", true)
                    .csv(outputPath);
        }finally{
            spark.stop();
        }
    }
}

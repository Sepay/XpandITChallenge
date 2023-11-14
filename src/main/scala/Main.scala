
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Main  {
  def main(args: Array[String]): Unit={
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    // Set Hadoop configuration to disable cache for local files
    spark.conf.set("spark.hadoop.fs.file.impl.disable.cache", "true")

    val csvPath = "googleplaystore_user_reviews.csv"
    val csvPath2 = "googleplaystore.csv"
    val df: DataFrame = spark.read.option("header", "true").csv(csvPath)
    val dfGoogle: DataFrame = spark.read.option("header", "true").csv(csvPath2)


    // Exercise part 1
    // Group by the "App" column and calculate the average sentiment polarity for each group and rounded to 3 decimal cases
    val averageSentimentByApp = df.groupBy("App")
      .agg(round(avg("Sentiment_Polarity"), 3).alias("Average_Sentiment"))

    //Final Dataframe with the null treatment.
    val df_1 = averageSentimentByApp.na.fill(0)

    // Show the result
    df_1.printSchema()
    df_1.show()


    val df_2 = dfGoogle
      .filter(col("Rating").isNotNull && col("Rating") >= 4.0 && !isnan(col("Rating")))
      .orderBy(col("Rating").desc)
    df_2.show() // The dataframe is correct

    /*

    df_2.coalesce(1)// Reduce the number of partitions
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", "§") // Delimiter
      .csv("C:\\Users\\SPY\\IdeaProjects\\DemoSpark\\best_apps")

    This code should ve worked, it is creating an empty csv file. My guess is that my setup of scala/spark is not the best one
    or the versions that I could do were not the best

    */


    //Exercise part 3


    // Transformations on specific columns
    val modifiedDF = dfGoogle
      .withColumn("Reviews", dfGoogle("Reviews").cast("Long"))
      .withColumn("Rating", dfGoogle("Rating").cast("Double"))
      .withColumn("Size", expr("CASE WHEN Size LIKE '%M%' THEN CAST(SUBSTRING(Size, 1, LENGTH(Size) - 1) AS Double) * 1000000 " +
        "WHEN Size LIKE '%K%' THEN CAST(SUBSTRING(Size, 1, LENGTH(Size) - 1) AS Double) * 1000 " +
        "ELSE CAST(Size AS Double) END")) // Changes both M and K number and the data that was string like "vaires with defice turns into null"
      .withColumn("Price", expr("IF(Price != 0, regexp_replace(Price, '\\$', '€') * 0.9, 0)"))
      .withColumn("Genres", split(col("Genres"), ";").cast("array<string>"))
      .withColumn("Last Updated", to_date(col("Last Updated"), "MMMM d, yyyy")) //Format Month/Day/Year because it was in that format from start.


    // Order by reviews and group by app and also modified the names that needed
    val resultReviewsDF = modifiedDF
      .orderBy(desc("Reviews"))
      .groupBy("App").agg(
        first("Category").as("Category"),
        first("Rating").as("Rating"),
        first("Reviews").as("Reviews"),
        first("Size").as("Size"),
        first("Installs").as("Installs"),
        first("Type").as("Type"),
        first("Price").as("Price"),
        first("Content Rating").as("Content_Rating"),
        first("Genres").as("Genres"),
        first("Last Updated").as("Last_Updated"),
        first("Current Ver").as("Current_Version"),
        first("Android Ver").as("Minimum_Android_Version")
      )

    // Convert the "Reviews" column to numeric type
    val appCategories = modifiedDF
      .groupBy("App")
      .agg(collect_list("Category").alias("Categories"))

    // Remove duplicates based on the "App" column
    val deduplicatedDF = modifiedDF.dropDuplicates("App")

    // Join the original DataFrame with the aggregated categories
    val resultDF = deduplicatedDF.join(appCategories, Seq("App"), "left_outer")
      .drop("Category") // Drop the old "Category" column

    // Select only the "App" and "Categories" columns from the result
    val categoriesDF = resultDF.select("App", "Categories")

    // Join the results on the "App" column
    val dF_3 = categoriesDF.join(resultReviewsDF, Seq("App"), "left_outer")

    // Show the final result
    dF_3.show()

    //Exercise part 4

    // Joining finalResultDF with averageSentimentByApp on the "App" column
    val dF_4 = dF_3.join(df_1, Seq("App"), "left_outer")
    dF_4.show() // Correct Dataframe

    /*
    dF_4.write
      .option("compression", "gzip")
      .parquet("googleplaystore_cleaned")
    Again It s creating the files but are empty but when I do the .show() command I can see the dataframe with all the data.
     I think my code is right but I have another guest that might be about my computer permissions. I ve never worked with this before so I dont know.*/


    //Exercise part 5
    //For this exercise instead of using the df_3 that doesnt contain the average sentiment polarity
    // I will use the dF_4 that is the df1 and 3 combined and create a new df5

    // Group by "Genres" and calculate averages and sum

    val explodedDF = dF_4.withColumn("Genre", explode(col("Genres")))

    val df_5 = explodedDF.groupBy("Genre").agg(
      count("App").alias("Count"),
      avg("Rating").alias("Average_Rating"),
      round(avg("Average_Sentiment" ),3).alias("Average_Sentiment_Polarity")
    )

    // The result of the correct dataframe
    df_5.show()

    /*
    df_5.write
      .option("compression", "gzip")
      .parquet("googleplaystore_metrics")
    */
    //Again this is the only thing I am not able to do.


    spark.stop()
  }
}

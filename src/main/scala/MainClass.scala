// package com.example.record_deduplicate

import org.apache.spark.sql.{SparkSession, SaveMode}

import org.apache.log4j.{Logger, Level}
import com.typesafe.config.ConfigFactory


object MainClass {

    // Load soft-coded configuration
    val conf = ConfigFactory.load()

    // Create spark instance
    implicit val spark: SparkSession = SparkSession.builder
        .appName(conf.getString("spark.name"))
        .master(conf.getString("spark.master"))
        .getOrCreate()

    import spark.implicits._

    // Reduce/adapt log level
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)


    def main (args: Array[String]) {

        // val rawBlocks = spark.sparkContext.textFile(conf.getString("data.path"))

        // println("> Number of rows (records + headers): " + rawBlocks.count)

        // rawBlocks.take(10) foreach println

        // // We use the fact that Header rows in the dataset contain the key "id_1" (among others) to distinguish between headers and and actual records
        // def isHeader(line: String): Boolean = line.contains("id_1")

        // val noheader = rawBlocks.filter(! isHeader(_))
        // // val noheader = rawBlocks.filterNot(isHeader(_))

        // println("> Number of records: " + noheader.count)


        // We'll let Spark infer the data types, but since this requires two passes over
        // the data, one should consider creating an instance of `StructType` and passe it
        // to the `schema` function of the Reader API.
        val parsed = spark.read
            .option("header", "true")
            .option("nullValue", "?")
            .option("inferSchema", "true")
            .csv(conf.getString("data.path"))
        // Also, given that the content of a DataFrame or RDD is frequently used, we would
        // like to cache/persist it into memory to reduce access latency, with taking into account
        // the available memory"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
            .cache()       // Or use .persist(StorageLevel.MEMORY)  // Or StorageLevel.MEMORY_SER for serialzed data

        parsed.show(5)
        parsed.printSchema()

        // Save as a Parquet file
        parsed.write.mode(SaveMode.Ignore).parquet(conf.getString("data/parquet"))

        // Count the matches vs non-matches
        // val match_number: scala.collections.Map[Boolean, Long] = parsed.rdd.map(_.getAs[Boolean]("is_match")).countByValue()
        parsed.groupBy("is_match").count().orderBy($"count".desc).show()
    }
}
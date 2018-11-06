// package com.example.record_deduplicate

import org.apache.spark.sql.SparkSession
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

    // Reduce/adapt log level
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)


    def main (args: Array[String]) {
        val rawBlocks = spark.sparkContext.textFile(conf.getString("data.path"))

        println("> Number of rows (records + headers): " + rawBlocks.count)

        rawBlocks.take(10) foreach println

        // We use the fact that Header rows in the dataset contain the key "id_1" (among others) to distinguish between headers and and actual records
        def isHeader(line: String): Boolean = line.contains("id_1")

        val noheader = rawBlocks.filter(! isHeader(_))
        // val noheader = rawBlocks.filterNot(isHeader(_))

        println("> Number of records: " + noheader.count)
    }
}
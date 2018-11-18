
// package com.example.record_deduplicate

import org.apache.spark.sql.SparkSession

import org.apache.log4j.{Logger, Level}
import com.typesafe.config.ConfigFactory


trait SparkMachinery {

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


}
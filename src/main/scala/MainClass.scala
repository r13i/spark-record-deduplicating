// package com.example.record_deduplicate

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{avg, stddev}


object MainClass extends SparkMachinery {

    def main (args: Array[String]) {

        import spark.implicits._



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
        // the available memory
            .cache()       // Or use .persist(StorageLevel.MEMORY)  // Or StorageLevel.MEMORY_SER for serialzed data

        parsed.show(5)
        parsed.printSchema()



        // // Save as a Parquet file
        // parsed.write.mode(SaveMode.Ignore).parquet(conf.getString("data.parquet"))




        // // Count the matches vs non-matches
        // // val match_number: scala.collections.Map[Boolean, Long] = parsed.rdd.map(_.getAs[Boolean]("is_match")).countByValue()
        // parsed.groupBy("is_match").count().orderBy($"count".desc).show()

        parsed.createOrReplaceTempView("linkage")

        spark.sql("""
            SELECT is_match, COUNT(*) cnt
            FROM linkage
            GROUP BY is_match
            ORDER BY cnt DESC
        """).show()




        // // Aggregate columns to average, standard dev, ...
        // parsed.agg(avg($"cmp_sex"), stddev($"cmp_sex"))



        // Calculate stats about the two classes (matching vs. non-matching) looking for disciminative features
        val matches = parsed.filter($"is_match" === true)   // We use === to compare column-wise
        val matchSummary = matches.describe()

        val misses = parsed.filter($"is_match" === false)
        val missSummary = misses.describe()

        matchSummary.show()
        missSummary.show()
    }
}
// package com.example.record_deduplicate

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{avg, stddev}

import Utils._


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

        // We pivot (transpose) the summary DataFrames 
        val matchSummaryT = pivotSummary(matchSummary)
        val missSummaryT = pivotSummary(missSummary)

        // Compare features for match vs. non-match summaries
        matchSummaryT.createOrReplaceTempView("match_desc")
        missSummaryT.createOrReplaceTempView("miss_desc")

        spark.sql("""
            SELECT a.field, a.count + b.count total, a.mean - b.mean delta
            FROM match_desc a INNER JOIN miss_desc b ON a.field = b.field
            WHERE a.field NOT IN ("id_1", "id_2")
            ORDER BY delta DESC, total DESC
        """).show

        println("> We see that are 4 types of features :")
        println(">> Features almost always available / With a neat difference between the two classes")
        println(">> Features almost always available / With little difference between the two classes")
        println(">> Features scarcely available / With a neat difference between the two classes")
        println(">> Features scarcely available / With little difference between the two classes")

        // Calculate match score based on `cmp_lname_c1`, `cmp_plz`, `cmp_by`, `cmp_bd`, `cmp_bm`
        // Casting the [[DataFrame]] to [[Dataset[MatchData]]], with `MatchData` a case class (@see Utils.scala)
        val matchData = parsed.as[MatchData]
        val scored = matchData.map { md =>
            (scoreMatchData(md), md.is_match)
        }.toDF("score", "is_match")

        val thresh = 4.0
        println(s"> The confusion matrix is given below for a discrimination threshold of $thresh")
        crossTabs(scored, thresh).show()
    }
}
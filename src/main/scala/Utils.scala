// package com.example.record_deduplicate


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.first


object Utils {

    /** Defines what a record `Row` is in a Spark-agnostic way to avoid having to deal with
      * Spark JARs once on production
      */
    case class MatchData (
        id_1            : Int,
        id_2            : Int,
        cmp_fname_c1    : Option[Double],
        cmp_fname_c2    : Option[Double],
        cmp_lname_c1    : Option[Double],
        cmp_lname_c2    : Option[Double],
        cmp_sex         : Option[Int],
        cmp_bd          : Option[Int],
        cmp_bm          : Option[Int],
        cmp_by          : Option[Int],
        cmp_plz         : Option[Int],
        is_match        : Boolean
    )

    /** Used to abstract away the boilerplate of calculating the score sum
      * @param value The first value of the running sum
      */
    case class Score(value: Double) {
        def +(oi: Option[Int]) = {
            Score(value + oi.getOrElse(0))
        }
    }

    /** Calculate the `Record Match Score` based on features that are most present in the dataset
      * and that have the highest discrimination capability
      * @param md Case class holding each record
      */
    def scoreMatchData (md: MatchData): Double = {
        (Score(md.cmp_lname_c1.getOrElse(0.0)) + md.cmp_plz + md.cmp_by + md.cmp_bd + md.cmp_bm).value
    }


    /** Calculate the confusion matrix (or contingency matrix) of a given prediction result, and a
      * given threshold
      * @param scored The prediction result
      * @param thresh The seperation the threshold to discriminate the two classes
      * @return A DataFrame holding the confusion matrix
      */
    def crossTabs (scored: DataFrame, thresh: Double): DataFrame = {
        scored
            .selectExpr(s"score >= $thresh as above", "is_match")
            .groupBy("above")
            .pivot("is_match", Seq("true", "false"))
            .count()
    }

    /** Helper function to transpose (pivot) a DataFrame returned by the `DataFrame.describe` method
      *
      * @param desc A DataFrame holding the summary, returned by the `DataFrame.describe` method
      * @return A DataFrame holding the pivoted description
      */
    def pivotSummary(desc: DataFrame): DataFrame = {

        // Import `toDF` from the SparkSession binded to the given description DataFrame
        import desc.sparkSession.implicits._

        val schema = desc.schema
        val longDF = desc.flatMap(row => {
            val metric = row.getString(0)
            (1 until row.size).map(i => {
                (metric, schema(i).name, row.getString(i).toDouble)
            })
        }).toDF("metric", "field", "value")

        longDF
            .groupBy("field")
            .pivot("metric", Seq("count", "mean", "stddev", "min", "max"))
            .agg(first("value"))
    }
}
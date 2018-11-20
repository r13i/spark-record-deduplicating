// package com.example.record_deduplicate


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.first


object Utils {

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
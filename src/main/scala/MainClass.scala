
import org.apache.spark.sql.SparkSession

object MainClass {

    implicit val spark: SparkSession = SparkSession.builder
        .appName("SomeFancyName")
        .master("local")
        .getOrCreate()

    def main (args: Array[String]) {
        println(123)
    }
}
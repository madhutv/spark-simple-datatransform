import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.singaj.mapdocreader.JSONMapDocReader
import org.singaj.simpletrans.ImplSimpleTransformer._
import org.singaj.mapdocreader.TransformationParser
import org.apache.spark.sql.functions._
/**
  * Created by madhu on 8/27/17.
  */
object UsageTest {

  def main(args: Array[String]): Unit = {

    //Read Jsons
    println("World Peace")
    //Usual Spark stuff
    val sc = new SparkConf().setAppName("Peace").setMaster("local")
    val spark = SparkSession.builder.config(sc).getOrCreate

    import spark.implicits._

    val json =  spark.read.textFile("resources/sample.json").collect().mkString
    val jsonT = new JSONMapDocReader(json)
    val tParser = new TransformationParser(jsonT)

    //Read StructType rules
    val struct = tParser.parseStructTypes
    //Get select columns from Json.

    val temp = tParser.parseTransformations


    //Read Dataset
    val initial = spark.read.format(tParser.inputFileFormat)
                            .option("delimiter", tParser.inputFileDelimiter)
                            .option("header", tParser.inputFileHasHeader)
                            .schema(struct)
                            .load("resources/test.csv")

    val joinin = spark.read.format("csv")
                           .option("header", true)
                           .load("resources/joinSample.csv")



  /*  val aggI = initial
                .groupBy("customer_id", "country")
                .agg(Map("InvoiceNo" -> "avg", "UnitPrice" -> "count"))

    aggI.show */

    val transformed = initial.stTransform(temp)

    transformed.show
    transformed.join(joinin).where(expr("PstockCode == jp and puma like '%OK%'")).show
    spark.stop
  }
}

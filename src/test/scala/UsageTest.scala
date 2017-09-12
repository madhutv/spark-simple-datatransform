import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.singaj.mapdocreader.JSONMapDocReader
import org.singaj.simpletrans.SimpleTransformer._
import org.apache.spark.sql.functions._
/**
  * Created by madhu on 8/27/17.
  */
object UsageTest {

  def main(args: Array[String]): Unit = {

    //Read Json
    println("World Peace")
    val jsonT = new JSONMapDocReader("resources/sample.json")

    //Read StructType rules
    val struct = jsonT.getFieldStructure
    //Get select columns from Json.

    val temp = jsonT.parseTransformations

    //Usual Spark stuff
    val sc = new SparkConf().setAppName("Peace").setMaster("local")
    val spark = SparkSession.builder.config(sc).getOrCreate

    import spark.implicits._

    //Read Dataset
    val initial = spark.read.format(jsonT.inputFileFormat)
                            .option("delimiter", jsonT.inputFileDelimiter)
                            .option("header", jsonT.inputFileHasHeader)
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

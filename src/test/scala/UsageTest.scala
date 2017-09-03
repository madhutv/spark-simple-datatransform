import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.singaj.mapdocreader.JSONMapDocReader
import org.singaj.simpletrans.SimpleTransformer._

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
    val selectCols = jsonT.getSelectColumns

    val temp = jsonT.parseTransformations

    //Usual Spark stuff
    val sc = new SparkConf().setAppName("Peace").setMaster("local")
    val spark = SparkSession.builder.config(sc).getOrCreate

    //Read Dataset
    val initial = spark.read.format(jsonT.inputFileFormat)
                            .option("delimiter", jsonT.inputFileDelimiter)
                            .option("header", jsonT.inputFileHasHeader)
                            .schema(struct)
                            .load("resources/test.csv")
  /*  val aggI = initial
                .groupBy("customer_id", "country")
                .agg(Map("InvoiceNo" -> "avg", "UnitPrice" -> "count"))

    aggI.show */

    val transformed = initial.stTransform(temp).stSelect(selectCols)

    transformed.show
    spark.stop
  }
}

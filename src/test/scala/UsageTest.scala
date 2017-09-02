import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.singaj.mapdocreader.JSONMapDocReader
import org.singaj.simpletrans.SimpleTransformer._

/**
  * Created by madhu on 8/27/17.
  */
object UsageTest {

  def main(args: Array[String]) = {

    //Read Json
    val jsonT = new JSONMapDocReader("resources/sample.json")
    //Read Transformation Rules
    val mappings = jsonT.getTransformations
    //Read StructType rules
    val struct = jsonT.getFieldStructure
    //Get select columns from Json.
    val selectCols = jsonT.getSelectColumns

    //Usual Spark stuff
    val sc = new SparkConf().setAppName("Peace").setMaster("local")
    val spark = SparkSession.builder.config(sc).getOrCreate

    //Read Dataset
    val initial = spark.read.format("csv").option("header", true).schema(struct).load("resources/test.csv")

    val transformed = initial.stTransform(mappings).stSelect(selectCols)

    transformed.show
    spark.stop
  }
}

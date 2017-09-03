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
    val jsonT = new JSONMapDocReader("resources/sample.json")
    //Read Transformation Rules
    val mappings = jsonT.getTransformations
    //Read StructType rules
    val struct = jsonT.getFieldStructure
    //Get select columns from Json.
    val selectCols = jsonT.getSelectColumns

   // val split = jsonT.getSplitLogic

    val temp = jsonT.parseTransformations
    temp.foreach(println)

    //Usual Spark stuff
    val sc = new SparkConf().setAppName("Peace").setMaster("local")
    val spark = SparkSession.builder.config(sc).getOrCreate

    //Read Dataset
    val initial = spark.read.format("csv").option("header", true).schema(struct).load("resources/test.csv")

    val transformed = initial.stTransform(temp).stSelect(selectCols)

    transformed.show
    spark.stop
  }
}

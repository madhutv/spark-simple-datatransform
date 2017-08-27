package org.singaj.test
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.singaj.mapdocreader.JSONMapDocReader
import org.singaj.rules.Transformations
import org.singaj.simpletrans.SimpleTransformer


/**
  * Created by madhu on 8/26/17.
  */
class GenericTester extends FunSuite {

  val mapper = new JSONMapDocReader("resources/sample.json")
  val mapperError = new JSONMapDocReader("resources/sampleError.json")
  val transformations = mapper.getTransformations
  val struct = mapper.getFieldStructure
  //Usual Spark stuff
  val sc = new SparkConf().setAppName("Peace").setMaster("local")
  val spark = SparkSession.builder.config(sc).getOrCreate
  import spark.implicits._

  //Read Dataset
  val initial = spark.read.format("csv").option("header", true).schema(struct).load("resources/2010-12-01.csv")
  val transformed = SimpleTransformer.transform(transformations, initial)
  val take5 = transformed.take(5)

  test("Create JSONMapDocReader with valid file must return mapper object"){
    assert(mapper.isInstanceOf[JSONMapDocReader])
  }

  test("Create JSONMapDocReader with invalid file must throw an Error"){
    assertThrows[Error](new JSONMapDocReader("Gibarish"))
  }

  test("GetTransformation on Sample.json must yield 5 transformations"){
    assert(transformations.length == 3)
  }

  test("GetTransformation on Sample.json must be of Transformation(ttype, rule, dest)"){
    assert(transformations.isInstanceOf[List[Transformations]])
  }

  test("First element from sample.json GetTransformation must be Transformation(DirectMap, Quantity, Q1)"){
    val firstT = transformations.head
    assert(firstT == Transformations("DirectMap", "Quantity", "Q1"))
  }

  test("GetTransformation on SampleError.jso must throw an Error"){
    assertThrows[Error](mapperError.getTransformations)
  }

  test("getFieldStructure on Sample.json must return StructType"){
    assert(struct.isInstanceOf[StructType])
  }

  test("getFieldStruct on Sample.json must return StructType with 8 StructField"){
    assert(struct.length == 8)
  }

  test("getFieldStruct on Sample.json must return First StructField as invoiceno, LongType, True"){
    assert(struct.head == StructField("invoiceno", LongType, true))
  }

  test("getFieldStruct on SampleError.json must StructField of 0 length"){
    assert(mapperError.getFieldStructure.length == 0)
  }

  test("simpleTransformer DirectMap on Quality should have produce Q1"){
    assert(transformed.select("Quantity").take(5) === transformed.select("Q1").take(5))
  }

  test("simpleTransformer DefaultMap on Currency should equal USD"){
    val curr = transformed.select("Currency").take(5)
    curr.foreach(c => assert(c.mkString == "USD"))
  }

}

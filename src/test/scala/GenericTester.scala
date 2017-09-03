package org.singaj.test
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.singaj.mapdocreader.JSONMapDocReader
import org.singaj.rules.{SimpleTransformation, Transformations}
import org.singaj.simpletrans.SimpleTransformer._



/**
  * Created by madhu on 8/26/17.
  */
class GenericTester extends FunSuite {

  val mapper = new JSONMapDocReader("resources/sample.json")
  val mapperError = new JSONMapDocReader("resources/sampleError.json")
  val transformations = mapper.parseTransformations
  val struct = mapper.getFieldStructure
  val selects = mapper.getSelectColumns
  //Usual Spark stuff
  val sc = new SparkConf().setAppName("Peace").setMaster("local")
  val spark = SparkSession.builder.config(sc).getOrCreate
  import spark.implicits._

  //Read Dataset
  val initial = spark.read.format("csv").option("header", true).schema(struct).load("resources/test.csv")
  val transformed = initial.stTransform(transformations)
  val take5 = transformed.take(3)

  test("Create JSONMapDocReader with valid file must return mapper object"){
    assert(mapper.isInstanceOf[JSONMapDocReader])
  }

  test("Create JSONMapDocReader with invalid file must throw an Error"){
    assertThrows[Error](new JSONMapDocReader("Gibarish"))
  }

  test("GetTransformation on Sample.json must yield 8 transformations"){
   // assert(transformations.length == 8)
  }

  test("GetTransformation on Sample.json must be of Transformation(ttype, rule, dest)"){
    assert(transformations.isInstanceOf[List[Transformations]])
  }

  test("First element from sample.json GetTransformation must be Transformation(DirectMap, Quantity, Q1)"){
    val firstT = transformations.head
    assert(firstT == SimpleTransformation(Some("DirectMap"), "Quantity", "Q1"))
  }

 /* test("GetTransformation on SampleError.jso must throw an Error"){
    assertThrows[Error](mapperError.getTransformations)
  }*/

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

  test("simpleTransformer ifElse case when Quantity = 6 then 9 else '' end Q1to9 must produce column" +
    "Q1to9 such that if Quality is 6, Q1to9 should be 9, else empty"){
    val curr = transformed.select("Quantity", "Q1to9").take(5)
    curr.foreach(c => assert((c(0) == 6 && c(1) == "9") || c(0) != 6 && c(1) == ""))
  }

  test("simpleTransformer concat stockCode, '*', Currency, '$' must produce column" +
    "CurStock such that if stockCode*Currency$"){
    val transformedCount = transformed.count
    val curr = transformed.filter("CurStock == concat(stockCode, '*', Currency, '$') ").count()
    assert(curr == transformedCount)
  }

  test("simpleTransformer expression case when Q1 = 6 then unitPrice else 0 end must product column" +
    "UPQ1 such as above"){
    val curr = transformed.select("Q1","unitPrice", "UPQ1").take(5)
    curr.foreach(c => assert((c(0) == 6 && c(2) == c(1)) || c(0) != 6 && c(2) == 0))
  }

  test("stSelect on UPQ1 as UPQ3, UPQ2, PstockCode, Q1, Q1to9 as Q1to92, must exactly return" +
    "UPQ3, UPQ2, PstockCode, Q1 and Q1to92 columns"){
    val expArr = Array("UPQ3", "UPQ2", "PstockCode", "Q1", "Q1to92")
    val cols = transformed.stSelect(selects).columns
    assert(cols.length == 5)
    expArr.foreach(a => assert(cols.contains(a)))

  }

}

package org.singaj.test
import org.scalatest.FunSuite
import org.apache.spark.sql.types.{StructType, StructField, LongType}
import org.singaj.mapdocreader.{JSONMapDocReader, Transformations}


/**
  * Created by madhu on 8/26/17.
  */
class GenericTester extends FunSuite {

  val mapper = new JSONMapDocReader("resources/sample.json")
  val mapperError = new JSONMapDocReader("resources/sampleError.json")
  val transformations = mapper.getTransformations
  val struct = mapper.getFieldStructure
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

}

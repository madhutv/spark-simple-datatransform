package org.singaj.mapdocreader
import org.apache.spark.sql.types._
import org.singaj.rules.{FieldStructure, Transformations}
/**
  * Created on 8/26/17.
  * Abstract class to read Mapping Document
  */
abstract class MapDocReader {

  /**
    * Abstract method to retrieve all transformations
    * @return List[Transformations]: List of transformations
    */
  def getTransformations: List[Transformations]

  /**
    * Abstract method to retrieve all FieldStructure
    * This will be used to build StructType for that can be used for spark schema.
    * Currently only following spark datatypes are handled:
    *   StringType
    *   IntegerType
    *   DoubleType
    *   LongType
    *   DateType
    *   BinaryType
    *   BooleanType
    *   TimestampType
    *   FloatType
    *   ByteType
    *   ShortType
    *   CalendarIntervalType
    *   NullType
    * If more complex and nested types are required, StructTypes has to be built the
    * usual spark way
    * @return StructType
    */
  def getFieldStructure: StructType

  /**
    * Reads input file to determine columns to be selected.
    * Input files may contain just the column name or column name along with renames.
    * @example  example of sample "UPQ1 as UPQ3, UPQ2, PstockCode, Q1, Q1to9 as Q1to92"
    * @return
    */
  def getSelectColumns: Array[String]

  /**
    * Recursive method takes in List of FieldStructure and generates List of spark StructField
    * @param fs List[FieldStructure]
    * @param sf List[StructField]
    * @return List of StructField
    */
  protected def structFieldBuilder(fs: List[FieldStructure], sf: List[StructField]): List[StructField] = {
    fs match {
      case Nil => sf.reverse
      case x::xs => {
        val structFields =  x match {
          case FieldStructure(a, "StringType", y) => StructField(a, StringType, y) :: sf
          case FieldStructure(a, "IntegerType", y) => StructField(a, IntegerType, y) :: sf
          case FieldStructure(a, "DoubleType", y) => StructField(a, DoubleType, y) :: sf
          case FieldStructure(a, "LongType", y) => StructField(a, LongType, y) :: sf
          case FieldStructure(a, "DateType", y) => StructField(a, DateType, y) :: sf
          case FieldStructure(a, "BinaryType", y) => StructField(a, BinaryType, y) :: sf
          case FieldStructure(a, "BooleanType", y) => StructField(a, BooleanType, y) :: sf
          case FieldStructure(a, "TimestampType", y) => StructField(a, TimestampType, y) :: sf
          case FieldStructure(a, "FloatType", y) => StructField(a, FloatType, y) :: sf
          case FieldStructure(a, "ByteType", y) => StructField(a, ByteType, y) :: sf
          case FieldStructure(a, "ShortType", y) => StructField(a, ShortType, y) :: sf
          case FieldStructure(a, "CalendarIntervalType", y) => StructField(a, CalendarIntervalType, y) :: sf
          case FieldStructure(a, "NullType", y) => StructField(a, NullType, y) :: sf
        }
        structFieldBuilder(xs, structFields )
      }
    }
  }

}

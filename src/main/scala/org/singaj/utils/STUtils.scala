package org.singaj.utils

/**
  * Created by madhu on 9/6/17.
  */
trait STUtils {

  /**
    * Function that can be used to check if option is not None.
    * If its None, error will be thrown, else value will be returned
    * @param opt: Option[A]: Option to check
    * @tparam A : Type of Option
    * @return
    */
  protected def getOrThrow[A](opt: Option[A]): A = {
    if(opt.isEmpty)
      throw new Error("Did not find required details ")
    else
      opt.get
  }


  /**
    * Check if columns match. Else throw error
    * @param dsCol: Checks if columns match for a given set of columns.
    *             This is used before mergining datasets
    */
  protected def checkColumnMatch(dsCol: Array[Array[String]]): Unit = {
    val len = dsCol.length
    var i = 0
    while(i < len - 1){
      if(dsCol(i).deep != dsCol(i+1).deep)
        throw new Error("Columns do not match " + dsCol)
      i = i + 1
    }
  }

  /**
    * Converts strings seperated by , to array. In the process, method also trim any extra spaces
    * @param str: String: Comma seperated string
    * @return : Array of Strings
    */
  protected def strToArr(str: String): Array[String] = {
    str.split(",").map(_.trim)
  }

  /**
    * Similar to overloaded method with String. Except it takes in Option and if Option
    * is empty, an empty array will be returned
    * @param str : Option[String] : Comma seperated option string
    * @return Array of string
    */
  protected def strToArr(str: Option[String]): Array[String] = {
    str match{
      case None => Array()
      case Some(a) => strToArr(a)
    }
  }

  /**
    * Throws an Error if the passed condition dose not satisfies
    * @param errMsg : Error message to be displayed
    * @param func : Function to be executed
    */
  protected def passOrThrow(errMsg: String = "Got an Error. Terminating")(func: () => Boolean) = {
    if(func() == false) throw new Error(errMsg)
  }



}

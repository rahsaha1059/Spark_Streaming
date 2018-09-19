package com.pwc.in.sparkstream

import org.apache.spark.AccumulatorParam
import scala.collection.mutable.Map

object StringAccumulatorParam extends AccumulatorParam[Map[String,String]] {
  
  def zero(initialValue: Map[String,String]): Map[String,String] = {
    
    Map(""->"")
  }
  def addInPlace(v1: Map[String,String], v2: Map[String,String]): Map[String,String] = {
    
    //v1+=(v2.keys.head -> v2.values.head)
    v1(v2.keys.head)= v1.get(v2.keys.head).toString()+"," + v2.values.head
    v1
  }
 
}
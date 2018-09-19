package com.pwc.in.sparkstream

import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.SparkConf
import scala.collection.mutable.StringBuilder
import org.apache.spark.Accumulator
import org.apache.spark.rdd.RDD
//import scala.collection.mutable.Map

object STBStreamApp {
  
  def prev_batch_computation(a:Accumulator[StringBuilder]) = {
    
    var s = a.value
    
    
    
  }
  
  def main(a:Array[String]){
    
    
   /* var id = 0
    val conf = new SparkConf().setAppName("STB Streaming App").setMaster("yarn-client")
    val kafkaParams = Map[String,String]("metadata.broker.list" -> "nn01.itversity.com:6667,nn02.itversity.com:6667,rm01.itversity.com:6667")
    val topic = Set("stbstream")
    
    val ssc = new StreamingContext(conf,Seconds(60))
    
    
    val accum= ssc.sparkContext.accumulator(scala.collection.mutable.Map("id"+id->" ; "))(StringAccumulatorParam)
   
    
    val msg:InputDStream[(String,String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, topic)
    
    //8 - IN/OUT
    val lines = msg.map(_._2)
    val lines_copy = msg.map(x=>{ id+=1
    val str = accum.value
    val prev_batch_in = str.get("id"+(id-1)).toString().split(",").toList 
    val prev_batch_in_mod = str.get("id"+(id-1)).getOrElse("").replace(',', '|').replace(';', ',').split("\\|").toList.map(x => (x.split(",")(0) , x.split(",")(1)))
    
    val prev_batch_in_rdd = ssc.sparkContext.parallelize(prev_batch_in_mod, 2) ; prev_batch_in_rdd})
    
    lines_copy.transform(rdd => {rdd.saveAsTextFile(a(1)+"id-"+id);rdd})
   // prev_batch_in_rdd.saveAsTextFile(a(1)+"id-"+id)
    
    //val filteredlines = lines.filter(_.split(",")(8).toUpperCase()=="IN")
    val filteredlines_in_out = lines.filter(x => x.split(",")(8).toUpperCase()=="IN" || x.split(",")(8).toUpperCase()=="OUT")
    // Logic for putting the uncoreleated events in a seperate storage(??)
    val group_in_out = filteredlines_in_out.transform(rdd => rdd.groupBy(x=>( x.split(",")(2),x.split(",")(5))))
    
  
   
    
    val prev_batch_in_update = group_in_out.transform(rdd => rdd.keys.filter(x => { val str = accum.value
    val prev_batch_in = str.get("id"+(id-1)).toString().split(",").toList ;val st= x._1+";"+x._2 ;!prev_batch_in.contains(st) }))
    prev_batch_in_update.transform(rdd => rdd.map(x=> {val stg=x._1+";"+x._2 ; accum+=scala.collection.mutable.Map("id"+id -> stg ) } ))
    val group_in_out_sort= group_in_out.transform(rdd => rdd.mapValues(x=> {val s=x.toList.sortBy(y => y.split(",")(1)).last; if(s.split(",")(8).equalsIgnoreCase("IN")) {val strg= s.split(",")(2)+ ";"+s.split(",")(5) ; accum+=scala.collection.mutable.Map("id"+id -> strg ) } }))
    val filteredlines = filteredlines_in_out.filter(_.split(",")(8).toUpperCase()=="IN")
    val filteredlines_dedupe= filteredlines.map(x=> (x.split(",")(2).trim(),x.split(",")(5).trim())).transform(rdd => rdd.distinct())
    
    val current_and_prev_union = filteredlines_dedupe.transform(rdd =>{ val str = accum.value
    val prev_batch_in = str.get("id"+(id-1)).toString().split(",").toList 
    val prev_batch_in_mod = str.get("id"+(id-1)).getOrElse("").replace(',', '|').replace(';', ',').split("\\|").toList.map(x => (x.split(",")(0) , x.split(",")(1)))
    
    val prev_batch_in_rdd = ssc.sparkContext.parallelize(prev_batch_in_mod, 2); rdd.union(prev_batch_in_rdd) })
    //val chnl= filteredlines_dedupe.map(x=>(x._2.trim().toUpperCase(),1))
    val chnl= current_and_prev_union.map(x=>(x._2.trim().toUpperCase(),1))
    val chnlviews = chnl.reduceByKey(_+_)
    val top5views = chnlviews.transform(rdd => { val list = rdd.sortBy(_._2, false).take(5) ; rdd.filter(a=> list.contains(a))})
    
    //chnlviews.saveAsTextFiles(a(0), "txt")
    top5views.saveAsTextFiles(a(0), "txt")
    ssc.start()
    ssc.awaitTermination()  */
    
    
    
    val conf = new SparkConf().setAppName("STB Streaming App").setMaster("yarn-client")
    val kafkaParams = Map[String,String]("metadata.broker.list" -> "nn01.itversity.com:6667,nn02.itversity.com:6667,rm01.itversity.com:6667")
    val topic = Set("stbstream")
    val ssc = new StreamingContext(conf,Seconds(60))
    
   
    val initial_file= List("","")
    val initial_file_1= ssc.sparkContext.parallelize(initial_file, 1)
    initial_file_1.saveAsTextFile(a(1)+"-id_0")
    
    val msg:InputDStream[(String,String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc, kafkaParams, topic)
    val lines = msg.map(_._2)
   
    val prev_batch_in_dstream = ssc.textFileStream("hdfs://nn01.itversity.com:8020/user/rahulsaha/streamdata").transform(rdd=> rdd.map(x=>{ (x.split(",")(0), x.split(",")(1)) }))
    val filteredlines_in_out = lines.filter(x => x.split(",")(8).toUpperCase()=="IN" || x.split(",")(8).toUpperCase()=="OUT")
    // Logic for putting the uncoreleated events in a seperate storage(??)
    val group_in_out = filteredlines_in_out.transform(rdd => rdd.groupBy(x=>( x.split(",")(2),x.split(",")(5))))
    val prev_batch_in_update = group_in_out.transform(rdd => rdd.keys)
    //prev_batch_in_update.transform(rdd => rdd.map(x=> {val stg=x._1+";"+x._2 ; accum+=scala.collection.mutable.Map("id"+id -> stg ) } ))
    val group_in_out_sort= group_in_out.transform(rdd => rdd.mapValues(x=> {val s=x.toList.sortBy(y => y.split(",")(1)).last; if(s.split(",")(8).equalsIgnoreCase("IN")) {List( s.split(",")(2)+ ","+s.split(",")(5) )  } else{ List("NA","NA") } }))
    val filterout_uncoreleated_in = group_in_out_sort.transform(rdd=> rdd.filter(x => !x._2(0).equalsIgnoreCase("NA")).keys)
    val filteredlines = filteredlines_in_out.filter(_.split(",")(8).toUpperCase()=="IN")
    val filteredlines_dedupe= filteredlines.map(x=> (x.split(",")(2).trim(),x.split(",")(5).trim())).transform(rdd => rdd.distinct())
    
    //updated previous and current uncoreleated in's
    val closed_stb_chnl_pair= group_in_out_sort.transform(rdd=> rdd.filter(x => x._2(0).equalsIgnoreCase("NA")).keys)
    val prev_current_in_updated= prev_batch_in_dstream.transformWith(closed_stb_chnl_pair, (rdd1:RDD[(String, String)],rdd2:RDD[(String, String)]) => rdd1.subtract(rdd2))
    
    prev_current_in_updated.saveAsTextFiles(a(1), "txt")
    
    val current_and_prev_union = filteredlines_dedupe.union(prev_batch_in_dstream)
    //val chnl= filteredlines_dedupe.map(x=>(x._2.trim().toUpperCase(),1))
    val chnl= current_and_prev_union.map(x=>(x._2.trim().toUpperCase(),1))
    val chnlviews = chnl.reduceByKey(_+_)
    val top5views = chnlviews.transform(rdd => { val list = rdd.sortBy(_._2, false).take(5) ; rdd.filter(a=> list.contains(a))})
    
    //chnlviews.saveAsTextFiles(a(0), "txt")
    top5views.saveAsTextFiles(a(0), "txt")
    ssc.start()
    ssc.awaitTermination()
    
    
    
  }
}
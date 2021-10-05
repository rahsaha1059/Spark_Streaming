package com.pwc.in

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.rdd.RDD

//Comment added
object SetTopBox1_6 extends App{

  val conf= new SparkConf().setAppName("STBApp1.6")
  val ssc = new StreamingContext(conf,Seconds(60))
  ssc.checkpoint("hdfs://nameservice1/user/srauser/stb_checkpoint_new2")
  
  val raw_input =ssc.textFileStream("/user/srauser/stblog")
  val filtered_in_out= raw_input.filter(x=> x.split(",")(8).equalsIgnoreCase("IN") || x.split(",")(8).equalsIgnoreCase("OUT"))
  
  
 /* val in_events = filtered_in_out.filter(x=> x.split(",")(8).equalsIgnoreCase("IN")).transform(rdd=> {val r=rdd.map(y=> (y.split(",")(2),y.split(",")(5)));r.distinct() })
  val group_by_chnl_views=in_events.transform(rdd=> rdd.groupBy(x=> x._2))
  val count_by_chnl_views = group_by_chnl_views.transform(rdd => rdd.mapValues(y=> y.count(x => true)))
  val update_state_dstream = count_by_chnl_views.updateStateByKey((s:Seq[Int],prev:Option[Int]) => { val v =s.sum + prev.getOrElse(0) ; Some(v)})
  val top_5_chnl_views = update_state_dstream.transform(rdd => {val arr=rdd.takeOrdered(5)(Ordering[Int].reverse.on(x=>x._2));rdd.sparkContext.parallelize(arr, rdd.partitions.length) } )
  top_5_chnl_views.saveAsTextFiles("hdfs://nameservice1/user/srauser/stbresult5/running_top_5_chanel_views", "txt")*/
  
  val filtered_in_out_imp_fields= filtered_in_out.map(x=>{val output= x.split(",");(output(1),output(2),output(5),output(8))})
  val grouped_unsorted=filtered_in_out_imp_fields.transform(rdd => rdd.groupBy(x=> (x._2,x._3)))
  val grouped_sorted = grouped_unsorted.transform(rdd=> rdd.mapValues(y=> y.toList.sortBy(x=> x._1)))
  
  val uncoreleated_in_pairs_current_batch =grouped_sorted.filter(x=> { 
    x match {
      case(t1,t2) => if(t2.last._4.equalsIgnoreCase("IN")) true else false 
      } 
    }).transform(rdd=> rdd.keys)
    
  val coreleated_in_pairs_current_batch =grouped_sorted.filter(x=> { 
    x match {
      case(t1,t2) => if(t2.last._4.equalsIgnoreCase("OUT")) true else false 
      } 
    }).transform(rdd=> rdd.keys)
  
  val u=uncoreleated_in_pairs_current_batch.map(x=> (x._1,x._2,"u","k"))
  val c= coreleated_in_pairs_current_batch.map(x=>(x._1,x._2,"c","k"))
  val u_union_c = u.union(c)
  val grouped_u_union_c = u_union_c.transform(rdd=>rdd.groupBy(y=>y._4))
  val list=grouped_u_union_c.map(x=> (x._1,x._2.toList))
  val list_final=list.updateStateByKey((s:Seq[List[(String,String,String,String)]],prev:Option[List[(String,String)]])=> { val c_list=s(0).filter(y=>y._3.equalsIgnoreCase("c")).map(x=> (x._1,x._2)); val u_list= s(0).filter(y=> y._3.equalsIgnoreCase("u")).map(x=> (x._1,x._2)) ; val prev_list=prev.getOrElse(Nil); val lst = prev_list.diff(c_list); val final_lst = lst.++(u_list); Some(final_lst)  })
  
  // (values- uncoreleated)+correleated+uncoreleated
  val values=list_final.transform(rdd=> {val r1=rdd.values; val r2=r1.flatMap(x=>{val tmp =x.mkString("|") ; val tmp2=tmp.split("\\|") ; tmp2}) ; r2.map(y=>{val m =y.split(",")(0);val n=y.split(",")(1) ; (m.replaceAll("\\(", ""), n.replaceAll("\\)", "") )})  })
  //val values=list_final.transform(rdd=> {val r1=rdd.values; val r2=r1.flatMap(x=>{val tmp =x.mkString("|") ; val tmp2=tmp.split("\\|") ; tmp2}) ; r2  })
  //values.saveAsTextFiles("hdfs://nameservice1/user/srauser/stbresult8/test", "txt")
  val values_subtract_unco= values.transformWith(uncoreleated_in_pairs_current_batch, (rdd1:RDD[(String,String)],rdd2:RDD[(String,String)])=> rdd1.subtract(rdd2))
  val co_union_unco= uncoreleated_in_pairs_current_batch.union(coreleated_in_pairs_current_batch)
  val final_union = values_subtract_unco.union(co_union_unco)
  // Top channel views computation considering the previous batches
  val group_by_chnl_views=final_union.transform(rdd=> rdd.groupBy(x=> x._2))
  val count_by_chnl_views = group_by_chnl_views.transform(rdd => rdd.mapValues(y=> y.count(x => true)))
  val top_5_chnl_views=count_by_chnl_views.transform(rdd => {val arr=rdd.takeOrdered(5)(Ordering[Int].reverse.on(x=>x._2));rdd.sparkContext.parallelize(arr, rdd.partitions.length) } )
  top_5_chnl_views.saveAsTextFiles("hdfs://nameservice1/user/srauser/stbresult8/batchwise_top_5_chanel_views", "txt")
  
  ssc.start()
  ssc.awaitTermination()
}
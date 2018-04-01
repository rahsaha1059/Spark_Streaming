package com.pwc.in

import scala.collection.mutable.Map
import scala.util.Random
import org.apache.log4j.Logger
import org.apache.log4j.PropertyConfigurator
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.sql.Date

case class SetTopBox(stbid:String,custid:String,siteid:String) extends Thread{
  
  val logger:Logger = Logger.getLogger(this.getClass().getName())
  
  override def run(){
    
    val timefrmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.ms")
    for(i<- 1 to (2+Random.nextInt(1000)))
    {
      val timestmpin= timefrmt.format(new Timestamp(System.currentTimeMillis()))
      val chanelname = DataGeneratorNew.channel.keys.toList(Random.nextInt(DataGeneratorNew.channel.size))
      val in = DataGeneratorNew.inEvent(timestmpin.split(" ")(1),chanelname)
      var message = timestmpin.split(" ")(0)+ ","+ timestmpin.split(" ")(1) +","+stbid+","+custid+","+siteid+","+ in +","+"IN"
      
      logger.info(message)
      Thread.sleep(2000+Random.nextInt((10000-2000).toInt))
      val timestmpout= timefrmt.format(new Timestamp(System.currentTimeMillis()))
      val out = DataGeneratorNew.inEvent(timestmpout.split(" ")(1),chanelname)
      message= timestmpout.split(" ")(0)+ ","+ timestmpout.split(" ")(1) +","+stbid+","+custid+","+siteid+","+ out +","+ "OUT"
      logger.info(message)
    }
    
    
  }
}



object DataGeneratorNew {
  
  val stb:Map[String,String] = Map("STB000001" -> "CUST000001,ST001" , 
      "STB000002" -> "CUST000002,ST001",
      "STB000003" -> "CUST000003,ST001",
      "STB000004" -> "CUST000004,ST001",
      "STB000005" -> "CUST000005,ST001",
      "STB000006" -> "CUST000006,ST001",
      "STB000007" -> "CUST000007,ST001",
      "STB000008" -> "CUST000008,ST001",
      "STB000009" -> "CUST000009,ST001",
      "STB000010" -> "CUST000010,ST001",
      "STB000011" -> "CUST000011,ST001",
      "STB000012" -> "CUST000012,ST001",
      "STB000013" -> "CUST000013,ST001",
      "STB000014" -> "CUST000014,ST001",
      "STB000015" -> "CUST000015,ST001",
      "STB000016" -> "CUST000016,ST001",
      "STB000017" -> "CUST000017,ST001",
      "STB000018" -> "CUST000018,ST001",
      "STB000019" -> "CUST000019,ST001",
      "STB000020" -> "CUST000020,ST001",
      "STB000021" -> "CUST000021,ST001",
      "STB000022" -> "CUST000022,ST001",
      "STB000023" -> "CUST000023,ST001",
      "STB000024" -> "CUST000024,ST001",
      "STB000025" -> "CUST000025,ST001",
      "STB000026" -> "CUST000026,ST001",
      "STB000027" -> "CUST000027,ST001",
      "STB000028" -> "CUST000028,ST001",
      "STB000029" -> "CUST000029,ST001",
      "STB000030" -> "CUST000030,ST001",
      "STB000031" -> "CUST000031,ST001",
      "STB000032" -> "CUST000032,ST001",
      "STB000033" -> "CUST000033,ST001",
      "STB000034" -> "CUST000034,ST001",
      "STB000035" -> "CUST000035,ST001",
      "STB000036" -> "CUST000036,ST001",
      "STB000037" -> "CUST000037,ST001",
      "STB000038" -> "CUST000038,ST001",
      "STB000039" -> "CUST000039,ST001",
      "STB000040" -> "CUST000040,ST001",
      "STB000041" -> "CUST000041,ST001",
      "STB000042" -> "CUST000042,ST001",
      "STB000043" -> "CUST000043,ST001",
      "STB000044" -> "CUST000044,ST001",
      "STB000045" -> "CUST000045,ST001",
      "STB000046" -> "CUST000046,ST001",
      "STB000047" -> "CUST000047,ST001",
      "STB000048" -> "CUST000048,ST001",
      "STB000049" -> "CUST000049,ST001",
      "STB000050" -> "CUST000050,ST001")
      
  val channel:Map[String,List[String]] = Map(
      "CHNL0001"->List("CHNL0001PRG001,10:30:00-11:00:00","CHNL0001PRG002,11:00:00-11:30:00","CHNL0001PRG003,11:30:00-12:00:00","CHNL0001PRG004,12:00:00-12:30:00","CHNL0001PRG005,12:30:00-13:00:00","CHNL0001PRG006,13:00:00-13:30:00","CHNL0001PRG007,13:30:00-14:00:00","CHNL0001PRG008,14:00:00-14:30:00"),
      "CHNL0002"->List("CHNL0002PRG001,10:30:00-11:00:00","CHNL0002PRG002,11:00:00-11:30:00","CHNL0002PRG003,11:30:00-12:00:00","CHNL0002PRG004,12:00:00-12:30:00","CHNL0002PRG005,12:30:00-13:00:00","CHNL0002PRG006,13:00:00-13:30:00","CHNL0002PRG007,13:30:00-14:00:00","CHNL0002PRG008,14:00:00-14:30:00"),
      "CHNL0003"->List("CHNL0003PRG001,10:30:00-11:00:00","CHNL0003PRG002,11:00:00-11:30:00","CHNL0003PRG003,11:30:00-12:00:00","CHNL0003PRG004,12:00:00-12:30:00","CHNL0003PRG005,12:30:00-13:00:00","CHNL0003PRG006,13:00:00-13:30:00","CHNL0003PRG007,13:30:00-14:00:00","CHNL0003PRG008,14:00:00-14:30:00"),
      "CHNL0004"->List("CHNL0004PRG001,10:30:00-11:00:00","CHNL0004PRG002,11:00:00-11:30:00","CHNL0004PRG003,11:30:00-12:00:00","CHNL0004PRG004,12:00:00-12:30:00","CHNL0004PRG005,12:30:00-13:00:00","CHNL0004PRG006,13:00:00-13:30:00","CHNL0004PRG007,13:30:00-14:00:00","CHNL0004PRG008,14:00:00-14:30:00"),
      "CHNL0005"->List("CHNL0005PRG001,10:30:00-11:00:00","CHNL0005PRG002,11:00:00-11:30:00","CHNL0005PRG003,11:30:00-12:00:00","CHNL0005PRG004,12:00:00-12:30:00","CHNL0005PRG005,12:30:00-13:00:00","CHNL0005PRG006,13:00:00-13:30:00","CHNL0005PRG007,13:30:00-14:00:00","CHNL0005PRG008,14:00:00-14:30:00"),
      "CHNL0006"->List("CHNL0006PRG001,10:30:00-11:00:00","CHNL0006PRG002,11:00:00-11:30:00","CHNL0006PRG003,11:30:00-12:00:00","CHNL0006PRG004,12:00:00-12:30:00","CHNL0006PRG005,12:30:00-13:00:00","CHNL0006PRG006,13:00:00-13:30:00","CHNL0006PRG007,13:30:00-14:00:00","CHNL0006PRG008,14:00:00-14:30:00"),
      "CHNL0007"->List("CHNL0007PRG001,10:30:00-11:00:00","CHNL0007PRG002,11:00:00-11:30:00","CHNL0007PRG003,11:30:00-12:00:00","CHNL0007PRG004,12:00:00-12:30:00","CHNL0007PRG005,12:30:00-13:00:00","CHNL0007PRG006,13:00:00-13:30:00","CHNL0007PRG007,13:30:00-14:00:00","CHNL0007PRG008,14:00:00-14:30:00"),
      "CHNL0008"->List("CHNL0008PRG001,10:30:00-11:00:00","CHNL0008PRG002,11:00:00-11:30:00","CHNL0008PRG003,11:30:00-12:00:00","CHNL0008PRG004,12:00:00-12:30:00","CHNL0008PRG005,12:30:00-13:00:00","CHNL0008PRG006,13:00:00-13:30:00","CHNL0008PRG007,13:30:00-14:00:00","CHNL0008PRG008,14:00:00-14:30:00"),
      "CHNL0009"->List("CHNL0009PRG001,10:30:00-11:00:00","CHNL0009PRG002,11:00:00-11:30:00","CHNL0009PRG003,11:30:00-12:00:00","CHNL0009PRG004,12:00:00-12:30:00","CHNL0009PRG005,12:30:00-13:00:00","CHNL0009PRG006,13:00:00-13:30:00","CHNL0009PRG007,13:30:00-14:00:00","CHNL0009PRG008,14:00:00-14:30:00"),
      "CHNL0010"->List("CHNL0010PRG001,10:30:00-11:00:00","CHNL0010PRG002,11:00:00-11:30:00","CHNL0010PRG003,11:30:00-12:00:00","CHNL0010PRG004,12:00:00-12:30:00","CHNL0010PRG005,12:30:00-13:00:00","CHNL0010PRG006,13:00:00-13:30:00","CHNL0010PRG007,13:30:00-14:00:00","CHNL0010PRG008,14:00:00-14:30:00"),
      "CHNL0011"->List("CHNL0011PRG001,10:30:00-11:00:00","CHNL0011PRG002,11:00:00-11:30:00","CHNL0011PRG003,11:30:00-12:00:00","CHNL0011PRG004,12:00:00-12:30:00","CHNL0011PRG005,12:30:00-13:00:00","CHNL0011PRG006,13:00:00-13:30:00","CHNL0011PRG007,13:30:00-14:00:00","CHNL0011PRG008,14:00:00-14:30:00"),
      "CHNL0012"->List("CHNL0012PRG001,10:30:00-11:00:00","CHNL0012PRG002,11:00:00-11:30:00","CHNL0012PRG003,11:30:00-12:00:00","CHNL0012PRG004,12:00:00-12:30:00","CHNL0012PRG005,12:30:00-13:00:00","CHNL0012PRG006,13:00:00-13:30:00","CHNL0012PRG007,13:30:00-14:00:00","CHNL0012PRG008,14:00:00-14:30:00"),
      "CHNL0013"->List("CHNL0013PRG001,10:30:00-11:00:00","CHNL0013PRG002,11:00:00-11:30:00","CHNL0013PRG003,11:30:00-12:00:00","CHNL0013PRG004,12:00:00-12:30:00","CHNL0013PRG005,12:30:00-13:00:00","CHNL0013PRG006,13:00:00-13:30:00","CHNL0013PRG007,13:30:00-14:00:00","CHNL0013PRG008,14:00:00-14:30:00"),
      "CHNL0014"->List("CHNL0014PRG001,10:30:00-11:00:00","CHNL0014PRG002,11:00:00-11:30:00","CHNL0014PRG003,11:30:00-12:00:00","CHNL0014PRG004,12:00:00-12:30:00","CHNL0014PRG005,12:30:00-13:00:00","CHNL0014PRG006,13:00:00-13:30:00","CHNL0014PRG007,13:30:00-14:00:00","CHNL0014PRG008,14:00:00-14:30:00"),
      "CHNL0015"->List("CHNL0015PRG001,10:30:00-11:00:00","CHNL0015PRG002,11:00:00-11:30:00","CHNL0015PRG003,11:30:00-12:00:00","CHNL0015PRG004,12:00:00-12:30:00","CHNL0015PRG005,12:30:00-13:00:00","CHNL0015PRG006,13:00:00-13:30:00","CHNL0015PRG007,13:30:00-14:00:00","CHNL0015PRG008,14:00:00-14:30:00"),
      "CHNL0016"->List("CHNL0016PRG001,10:30:00-11:00:00","CHNL0016PRG002,11:00:00-11:30:00","CHNL0016PRG003,11:30:00-12:00:00","CHNL0016PRG004,12:00:00-12:30:00","CHNL0016PRG005,12:30:00-13:00:00","CHNL0016PRG006,13:00:00-13:30:00","CHNL0016PRG007,13:30:00-14:00:00","CHNL0016PRG008,14:00:00-14:30:00"),
      "CHNL0017"->List("CHNL0017PRG001,10:30:00-11:00:00","CHNL0017PRG002,11:00:00-11:30:00","CHNL0017PRG003,11:30:00-12:00:00","CHNL0017PRG004,12:00:00-12:30:00","CHNL0017PRG005,12:30:00-13:00:00","CHNL0017PRG006,13:00:00-13:30:00","CHNL0017PRG007,13:30:00-14:00:00","CHNL0017PRG008,14:00:00-14:30:00"),
      "CHNL0018"->List("CHNL0018PRG001,10:30:00-11:00:00","CHNL0018PRG002,11:00:00-11:30:00","CHNL0018PRG003,11:30:00-12:00:00","CHNL0018PRG004,12:00:00-12:30:00","CHNL0018PRG005,12:30:00-13:00:00","CHNL0018PRG006,13:00:00-13:30:00","CHNL0018PRG007,13:30:00-14:00:00","CHNL0018PRG008,14:00:00-14:30:00"),
      "CHNL0019"->List("CHNL0019PRG001,10:30:00-11:00:00","CHNL0019PRG002,11:00:00-11:30:00","CHNL0019PRG003,11:30:00-12:00:00","CHNL0019PRG004,12:00:00-12:30:00","CHNL0019PRG005,12:30:00-13:00:00","CHNL0019PRG006,13:00:00-13:30:00","CHNL0019PRG007,13:30:00-14:00:00","CHNL0019PRG008,14:00:00-14:30:00"),
      "CHNL0020"->List("CHNL0020PRG001,10:30:00-11:00:00","CHNL0020PRG002,11:00:00-11:30:00","CHNL0020PRG003,11:30:00-12:00:00","CHNL0020PRG004,12:00:00-12:30:00","CHNL0020PRG005,12:30:00-13:00:00","CHNL0020PRG006,13:00:00-13:30:00","CHNL0020PRG007,13:30:00-14:00:00","CHNL0020PRG008,14:00:00-14:30:00"),
      "CHNL0021"->List("CHNL0021PRG001,10:30:00-11:00:00","CHNL0021PRG002,11:00:00-11:30:00","CHNL0021PRG003,11:30:00-12:00:00","CHNL0021PRG004,12:00:00-12:30:00","CHNL0021PRG005,12:30:00-13:00:00","CHNL0021PRG006,13:00:00-13:30:00","CHNL0021PRG007,13:30:00-14:00:00","CHNL0021PRG008,14:00:00-14:30:00"),
      "CHNL0022"->List("CHNL0022PRG001,10:30:00-11:00:00","CHNL0022PRG002,11:00:00-11:30:00","CHNL0022PRG003,11:30:00-12:00:00","CHNL0022PRG004,12:00:00-12:30:00","CHNL0022PRG005,12:30:00-13:00:00","CHNL0022PRG006,13:00:00-13:30:00","CHNL0022PRG007,13:30:00-14:00:00","CHNL0022PRG008,14:00:00-14:30:00"),
      "CHNL0023"->List("CHNL0023PRG001,10:30:00-11:00:00","CHNL0023PRG002,11:00:00-11:30:00","CHNL0023PRG003,11:30:00-12:00:00","CHNL0023PRG004,12:00:00-12:30:00","CHNL0023PRG005,12:30:00-13:00:00","CHNL0023PRG006,13:00:00-13:30:00","CHNL0023PRG007,13:30:00-14:00:00","CHNL0023PRG008,14:00:00-14:30:00"),
      "CHNL0024"->List("CHNL0024PRG001,10:30:00-11:00:00","CHNL0024PRG002,11:00:00-11:30:00","CHNL0024PRG003,11:30:00-12:00:00","CHNL0024PRG004,12:00:00-12:30:00","CHNL0024PRG005,12:30:00-13:00:00","CHNL0024PRG006,13:00:00-13:30:00","CHNL0024PRG007,13:30:00-14:00:00","CHNL0024PRG008,14:00:00-14:30:00"),
      "CHNL0025"->List("CHNL0025PRG001,10:30:00-11:00:00","CHNL0025PRG002,11:00:00-11:30:00","CHNL0025PRG003,11:30:00-12:00:00","CHNL0025PRG004,12:00:00-12:30:00","CHNL0025PRG005,12:30:00-13:00:00","CHNL0025PRG006,13:00:00-13:30:00","CHNL0025PRG007,13:30:00-14:00:00","CHNL0025PRG008,14:00:00-14:30:00"),
      "CHNL0026"->List("CHNL0026PRG001,10:30:00-11:00:00","CHNL0026PRG002,11:00:00-11:30:00","CHNL0026PRG003,11:30:00-12:00:00","CHNL0026PRG004,12:00:00-12:30:00","CHNL0026PRG005,12:30:00-13:00:00","CHNL0026PRG006,13:00:00-13:30:00","CHNL0026PRG007,13:30:00-14:00:00","CHNL0026PRG008,14:00:00-14:30:00"),
      "CHNL0027"->List("CHNL0027PRG001,10:30:00-11:00:00","CHNL0027PRG002,11:00:00-11:30:00","CHNL0027PRG003,11:30:00-12:00:00","CHNL0027PRG004,12:00:00-12:30:00","CHNL0027PRG005,12:30:00-13:00:00","CHNL0027PRG006,13:00:00-13:30:00","CHNL0027PRG007,13:30:00-14:00:00","CHNL0027PRG008,14:00:00-14:30:00"),
      "CHNL0028"->List("CHNL0028PRG001,10:30:00-11:00:00","CHNL0028PRG002,11:00:00-11:30:00","CHNL0028PRG003,11:30:00-12:00:00","CHNL0028PRG004,12:00:00-12:30:00","CHNL0028PRG005,12:30:00-13:00:00","CHNL0028PRG006,13:00:00-13:30:00","CHNL0028PRG007,13:30:00-14:00:00","CHNL0028PRG008,14:00:00-14:30:00"),
      "CHNL0029"->List("CHNL0029PRG001,10:30:00-11:00:00","CHNL0029PRG002,11:00:00-11:30:00","CHNL0029PRG003,11:30:00-12:00:00","CHNL0029PRG004,12:00:00-12:30:00","CHNL0029PRG005,12:30:00-13:00:00","CHNL0029PRG006,13:00:00-13:30:00","CHNL0029PRG007,13:30:00-14:00:00","CHNL0029PRG008,14:00:00-14:30:00"),
      "CHNL0030"->List("CHNL0030PRG001,10:30:00-11:00:00","CHNL0030PRG002,11:00:00-11:30:00","CHNL0030PRG003,11:30:00-12:00:00","CHNL0030PRG004,12:00:00-12:30:00","CHNL0030PRG005,12:30:00-13:00:00","CHNL0030PRG006,13:00:00-13:30:00","CHNL0030PRG007,13:30:00-14:00:00","CHNL0030PRG008,14:00:00-14:30:00"),
      "CHNL0031"->List("CHNL0031PRG001,10:30:00-11:00:00","CHNL0031PRG002,11:00:00-11:30:00","CHNL0031PRG003,11:30:00-12:00:00","CHNL0031PRG004,12:00:00-12:30:00","CHNL0031PRG005,12:30:00-13:00:00","CHNL0031PRG006,13:00:00-13:30:00","CHNL0031PRG007,13:30:00-14:00:00","CHNL0031PRG008,14:00:00-14:30:00"),
      "CHNL0032"->List("CHNL0032PRG001,10:30:00-11:00:00","CHNL0032PRG002,11:00:00-11:30:00","CHNL0032PRG003,11:30:00-12:00:00","CHNL0032PRG004,12:00:00-12:30:00","CHNL0032PRG005,12:30:00-13:00:00","CHNL0032PRG006,13:00:00-13:30:00","CHNL0032PRG007,13:30:00-14:00:00","CHNL0032PRG008,14:00:00-14:30:00")
      )
  
  def inEvent(t:String,channelname:String):String = {
    
    val prgmlist = channel(channelname)
    val sdf = new SimpleDateFormat("HH:mm:ss")
    
    var prgmdetail=""
    for(i <- prgmlist)
    {
      val prg= i.split(",")
      val d = sdf.parse(t)
      val startd = sdf.parse(prg(1).split("-")(0))
      val endd = sdf.parse(prg(1).split("-")(1))
      if (startd.getTime<=d.getTime && d.getTime<=endd.getTime){
        prgmdetail = channelname+","+prg(0)+","+prg(1)
      }
    }
    if(prgmdetail == "")
      prgmdetail= channelname+","+"PG000"+","+"TIME000"
      
    prgmdetail
    
  }
  
  // to add def outEvent
  
  def main(a:Array[String]){
    
    PropertyConfigurator.configure("log4j.properties")
    // collection of stb's and loop through 
    val start_sleep:Long = 1000
    val end_sleep:Long = 2000
    
    val itr = stb.iterator
    while( itr.hasNext )
    {
      var tpl = itr.next()
      SetTopBox(tpl._1,tpl._2.split(",")(0),tpl._2.split(",")(1)).start()
      
      Thread.sleep(start_sleep+Random.nextInt((end_sleep-start_sleep).toInt))
      
    }
  }
}
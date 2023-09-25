import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object wordcount extends App{
  
  Logger.getLogger("org").setLevel(Level.INFO)
  val sc = new SparkContext("local[*]","wordcount")
  val input=sc.textFile("D:/Desktop data 2023/Desktop/BigData/student-data.txt")
  val words=input.flatMap(x=>x.split(" "))
  val wordMap=words.map(x=>(x,1))
  val result=wordMap.reduceByKey((x,y)=>x+y)
  val mycount=result.collect()
  mycount.foreach(println)
  
  
}
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object movieRatings extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","movieRatings")
  val ratingsRdd=sc.textFile("D:/Desktop data 2023/Desktop/Bharti-BigData/ratings.dat")
  val moviesRdd=sc.textFile("D:/Desktop data 2023/Desktop/Bharti-BigData/movies.dat")
  
  val mapMovies=moviesRdd.map(x=> (x.split("::")(0).toInt,x.split("::")(1).toString))
  val mapRatings=ratingsRdd.map(x=> (x.split("::")(1).toInt,1))
  
  val reduceRatings=mapRatings.reduceByKey((x,y)=> x+y)
  
  val filteredRating=reduceRatings.filter(x=> x._2>999)
  
  val joinedRdd=filteredRating.join(mapMovies)
  
  val namesRdd=joinedRdd.map(x=>(x._1,x._2._2))
  
  val avgRatings=ratingsRdd.map(x=>(x.split("::")(1).toInt,x.split("::")(2).toInt))
  
  val mapAvgRatings=avgRatings.mapValues(x=>(x,1))
  
  val ratings=mapAvgRatings.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
  
  val finalAvgRating=ratings.mapValues(x=>x._1/x._2.toFloat)
  
  val finalResult=finalAvgRating.filter(x=> x._2 > 4.5)
  
  val joinRdd2=finalResult.join(mapMovies)
  
  val joinRddRating=joinRdd2.map(x=>x._1)
  val result=joinRddRating.collect()
  
  val finalRdd=namesRdd.filter(x=> result.contains(x._1))
  finalRdd.collect.foreach(println)
  
  
  
  
  
}
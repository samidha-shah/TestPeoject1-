import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions


object Pactical10parallie extends App  {
  
  val sc = new SparkContext("local[*]","rdd")
  
  //below is local collection 
  val myList = List("WARN: Tuesday 4 September 0405",
"ERROR: Tuesday 4 September 0408",
"ERROR: Tuesday 4 September 0408",
"ERROR: Tuesday 4 September 0408",
"ERROR: Tuesday 4 September 0408",
"ERROR: Tuesday 4 September 0408")

// we have to change local to RDD
val originalLogs = sc.parallelize(myList)

val newRdd= originalLogs.map(x => {
  val col = x.split(":")(0)
  (col,1)
})
val result = newRdd.reduceByKey((x,y) => x+y)

result.collect().foreach(println)
  
  
}
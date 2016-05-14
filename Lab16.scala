import scala.math.pow
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf


case class Lab16Results(convergence:Double,kPoints:Array[(Double,Double)])
trait Lab16Stub extends Serializable{
  //======================== UTILITY METHODS=========================    
// The squared distances between two points
def distanceSquared(p1: (Double,Double), p2: (Double,Double)) = {
  pow(p1._1 - p2._1,2) + pow(p1._2 - p2._2,2 )
}
 
// The sum of two points
def addPoints(p1: (Double,Double), p2: (Double,Double)) = {
  (p1._1 + p2._1, p1._2 + p2._2)
}
 
// for a point p and an array of points, return the index in the array of the point closest to p
def closestPoint(p: (Double,Double), points: Array[(Double,Double)]): Int = {
    var index = 0
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    for (i <- 0 until points.length) {
      val dist = distanceSquared(p,points(i))
      if (dist < closest) {
        closest = dist
        bestIndex = i
      }
    }
    bestIndex
}
  //===================================================================
  def run(rawData:RDD[String]):Lab16Results
}
object Lab16 extends Lab16Stub{
    
 def debugRDD[T](title: String, rdd:RDD[T], n:Int=5){
   println(s"======= $title =======")
   rdd.take(n)foreach(println)
   println("=====================")
  }
  /**
   * Refer to lap 16 homework sheet and implement this method
   * you can use debugRDD to print the first n rows (n = 5 by default)
   * @return a case class containing :
   * <ol>
   * <li> The convergence value that stops the iteration </li>
   * <li> The final k means, each mean is represented by a tuple of (lat,long)</li> 
   * </ol> 
   */
  def run(rawData:RDD[String]):Lab16Results={     
    // ========YOUR CODE GOES HERE===========
    //************************ PREPARE DATA ********************
    //YOU NEED TO HAVE /loudacre/devicestatus_etl folder ready in your hdfs
    //PLEASE REFER TO THE SCRIPT IN http://pastebin.com/CmKd0ayP FOR HELP
    //**********************************************************
    println("Lab16 : K-Means Clustering")
    debugRDD("Debuging Input Data",rawData)
    // K is the number of means (center points of clusters) to find
    val K = 5
 
    // ConvergeDist -- the threshold "distance" between iterations at which we decide we are done
    val convergeDist = .1
    
    // Parse the device status data file into pairs
    // TODO
    //val devs = sc.textFile("/loudacre/devicestatus_etl/*")

    //val devlocs = devs.map(y => (y.split(",")(3),y.split(",")(4))).filter{case (k,v)=> v != 0.0}
    val devlocs = rawData.map(y => (y.split(",")(3).toDouble,y.split(",")(4).toDouble)).filter{case (k,v)=> v != 0.0}

    //start with K randomly selected points from the dataset
    //TODO

    val kPoints:Array[(Double,Double)] = devlocs.takeSample(withReplacement = false, K, 42).toArray

	// set the value with K random points
    //  loop until the total distance between one iteration's points and the next is less than the convergence distance specified
    var tempDist = Double.PositiveInfinity
    println("Lab16")
    while (tempDist > convergeDist) {


      // for each point, find the index oclosestPointf the closest kpoint.  map to (index, (point,1))
      //TODO

 	val closest = devlocs.map (p => (closestPoint(p, kPoints), (p, 1)))


      // For each key (k-point index), reduce by adding the coordinates and number of points
      //TODO

       	val pointStats = closest.reduceByKey{case ((p1,c1),(p2,c2)) => (addPoints(p1,p2),c1+c2) }



      // For each key (k-point index), find a new point by calculating the average of each closest point
      //TODO
   	val newPoints = pointStats.map{case (i,(point,n)) => (i,(point._1/n,point._2/n))}.sortByKey().collect()
	var newPointsavg = newPoints.map(value => value._2)
      // calculate the total of the distance between the current points and new points
      // TODO

	tempDist = 0.0
   	for (i <- 0 until K) {
		tempDist += distanceSquared(kPoints(i), newPointsavg(i))
	}

      // Copy the new points to the kPoints array for the next iteration
      // TODO
	for (i <- 0 until K) {
		kPoints(i) = newPointsavg(i)
	}

      //TODO : REMOVE THIS LINE TO CONTINUE LOOPING
      //tempDist= -1.0;
    }


    println("Lab16 DONE!")

    return Lab16Results(tempDist,kPoints)
  }
  def runOnShell(sc:SparkContext){
    // Disabling verbose messages
    org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level.WARN)
    val file = "hdfs://localhost:8020/loudacre/devicestatus_etl";
    val data = sc.textFile(file); 
    run(data)
  }
  // YOU CAN RUN THIS FILE FROM ECLIPSE BY : right click on the file and : Run As-> Scala Application
   def main(args: Array[String]): Unit = { 
     val conf = new org.apache.spark.SparkConf()
      .setAppName("The swankiest Spark app ever")
      .setMaster("local[1]")
     val sc  =  new SparkContext(conf)
     runOnShell(sc);
  }
}
// To test your code on spark-shell, run those lines in the spark-shell 
// :load Lab16.scala
// Lab16.runOnShell(sc)

 


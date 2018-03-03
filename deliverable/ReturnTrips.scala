import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import scala.collection.mutable.Map


object ReturnTrips {
  def compute(trips : Dataset[Row], dist : Double, spark : SparkSession) : Dataset[Row] = {

    import spark.implicits._
    
    
	
	val R = 6371 //radius in km
 
	def haversine(lat1:Double, lon1:Double, lat2:Double, lon2:Double)={
      val dLat=(lat2 - lat1).toRadians
      val dLon=(lon2 - lon1).toRadians
 
      val a = Math.pow(Math.sin(dLat/2),2) + Math.pow(Math.sin(dLon/2),2) * Math.cos(lat1.toRadians) * Math.cos(lat2.toRadians)
      val c = 2 * Math.asin(Math.sqrt(a))
      Math.abs(R * c * 1000)
	}
	
	val dist_func = udf(haversine _)

    val interval = 0.001 * dist / 111.2
	//val trips_disp = trips.withColumn("Displacement",dist_func($"pickup_latitude",$"pickup_longitude",$"dropoff_latitude",$"dropoff_longitude")).cache()

	//val trips_filter = trips_disp.select('tpep_pickup_datetime,'tpep_dropoff_datetime,'pickup_longitude,'pickup_latitude,'dropoff_longitude,'dropoff_latitude,'Displacement).cache()
    val trips_filter = trips.select('tpep_pickup_datetime,'tpep_dropoff_datetime,'pickup_longitude,'pickup_latitude,'dropoff_longitude,'dropoff_latitude)
	val trips_buck = trips_filter.withColumn("Buckets_plat",floor($"pickup_latitude"/interval)).withColumn("Buckets_dlat",floor($"dropoff_latitude"/interval)).withColumn("Buckets_ptime",floor(unix_timestamp($"tpep_pickup_datetime")/28800)).withColumn("Buckets_dtime",floor(unix_timestamp($"tpep_dropoff_datetime")/28800))

	//val trips_buckNeighbors = trips_buck.withColumn("Buckets", explode(array($"Buckets" - 1, $"Buckets", $"Buckets" + 1))).cache()

    val trips_buckNeighbors = trips_buck.withColumn("Buckets_plat", explode(array($"Buckets_plat" - 1, $"Buckets_plat", $"Buckets_plat" + 1))).withColumn("Buckets_dlat", explode(array($"Buckets_dlat" - 1, $"Buckets_dlat", $"Buckets_dlat" + 1))).withColumn("Buckets_ptime", explode(array($"Buckets_ptime", $"Buckets_ptime" - 1)))
    
    val return_trips = trips_buck.as("a").join(trips_buckNeighbors.as("b"), 
    ($"a.Buckets_dlat" === $"b.Buckets_plat")
    &&($"b.Buckets_dlat" === $"a.Buckets_plat") 
    && ($"a.Buckets_dtime" === $"b.Buckets_ptime") 
    && (abs(dist_func($"a.dropoff_latitude",$"a.dropoff_longitude",$"b.pickup_latitude",$"b.pickup_longitude")) < dist)
    && (abs(dist_func($"b.dropoff_latitude",$"b.dropoff_longitude",$"a.pickup_latitude",$"a.pickup_longitude")) < dist)
    && ((unix_timestamp($"b.tpep_pickup_datetime")-unix_timestamp($"a.tpep_dropoff_datetime")) > 0)
    && ((unix_timestamp($"a.tpep_dropoff_datetime")+ 8*60*60) > unix_timestamp($"b.tpep_pickup_datetime")))
    return_trips
  }
}

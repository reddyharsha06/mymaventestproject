package cg.samples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}
//import org.apache.log4j
//import sqlContext.implicits._
import org.apache.spark.sql.functions._

object ReadCSVFileDF {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("ReadCSVFile")
      .config("spark.master", "local")
      .getOrCreate()


    val mySchema = new StructType(Array(new StructField("AIRLINE",StringType,true)
      ,StructField("AIRLINE_ID",StringType, true)
      ,StructField("SOURCE_AIRPORT", StringType,true)
      ,StructField("SOURCE_AIRPORT_ID",StringType,true)
      ,StructField("DEST_AIRPORT",StringType,true)
      ,StructField("DEST_AIRPORT_ID",StringType,true)
      ,StructField("CODESHARE",StringType,true)
      ,StructField("STOPS",StringType,true)
      ,StructField("EQUIPMENT",StringType,true)))

    val df = spark.read.format("csv").schema(mySchema).load("/Users/harsha/Datasets/routes.csv")

    //df.select("*").show(10)

    val header = df.first()

    val dfWithNoHeader = df.filter{ row => row != header}

   // dfWithNoHeader.select("*").show(50)

   // dfWithNoHeader.groupBy("SOURCE_AIRPORT", "DEST_AIRPORT").count().orderBy(("count")).show(50)

    dfWithNoHeader.groupBy("AIRLINE").count().orderBy(desc("count")).show(100)


  }
}


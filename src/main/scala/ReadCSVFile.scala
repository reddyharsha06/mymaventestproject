import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object ReadCSVFile extends App {

 // val conf = new SparkConf().setMaster("local[*]").setAppName("ReadCSVFile")

 // val sc = new SparkContext(conf)

  //val warehouseLocation = "file:${system:user.dir}/spark-warehouse"
  val spark = SparkSession
    .builder()
    .appName("ReadCSVFile")
    .config("spark.master", "local")
    //.master("local[*]")
   // .config("spark.sql.warehouse.dir", warehouseLocation)
    .getOrCreate()

 // val readFile = sc.textFile("/Users/harsha/Datasets/routes.csv")

 // readFile.take(10).foreach(println)


  val mySchema = new StructType(Array(new StructField("AIRLINE",StringType,true)
    ,StructField("AIRLINE_ID",StringType, true)
    ,StructField("SOURCE_AIRPORT", StringType,true)
    ,StructField("SOURCE_AIRPORT_ID",StringType,true)
    ,StructField("DEST_AIRPORT",StringType,true)
    ,StructField("DEST_AIRPORT_ID",StringType,true)
    ,StructField("CODESHARE",StringType,true)
    ,StructField("STOPS",StringType,true)
    ,StructField("EQUIPMENT",StringType,true)))

  val df = spark
    .read
    .format("csv")
    .schema(mySchema)
    .option("header","true")
}

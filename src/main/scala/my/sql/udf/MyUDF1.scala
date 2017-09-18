package my.sql.udf

import org.apache.spark.sql.SparkSession

/**
  * local模式下，读取远端的hive数据
  */
object MyUDF1 {
  case class Record(search_word: String, url: String, search_kind: String)

  def main(args: Array[String]): Unit = {
    // warehouseLocation points to the default location for managed databases and tables
    val warehouseLocation = "hdfs://bdp-02:8020/user/hive/warehouse/hadoop";

    System.setProperty("HADOOP_USER_NAME", "hadoop")
    //  driver本来是要放在集群中运行的。现在在本地运行，会导致很多配置文件确实。需要补充。例如从hivemetadata获取到了表路径webankcluster就不认识。
    val spark = SparkSession
      .builder()
      //.master("spark://192.168.56.103:7077")
      .master("local[*]")
      .appName("MyUDF1")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.uris", "thrift://bdp-03:9083")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.sql("SELECT * FROM spark_test.sogoue")
    println(df.schema)
    df.show()

    df.createTempView("sogoue")

    import java.util.Date
    //函数体
    val formatDate = (str:String) => {
      import java.text.SimpleDateFormat
      val sdf = new SimpleDateFormat("yyyy-MM-dd")
      val date = sdf.parse(str)
      sdf.format(date)
    }

    println(formatDate("2017-09-18 16:07:01"))

    spark.udf.register("formatDate", formatDate)

    /** ********** now **************/
    //val result = spark.sql("SELECT *, formatDate(now()) FROM sogoue")
    val result = spark.sql("SELECT *, formatDate(now()) FROM sogoue")
    result.show()

    spark.stop()
  }
}
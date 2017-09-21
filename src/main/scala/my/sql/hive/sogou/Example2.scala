package my.sql.hive.sogou

import org.apache.spark.sql.SparkSession

object Example2 {

  def main(args: Array[String]) {
    // warehouseLocation points to the default location for managed databases and tables
    val warehouseLocation = "hdfs://bdp-02:8020/user/hive/warehouse/hadoop";

    System.setProperty("HADOOP_USER_NAME", "hadoop")
    //  driver本来是要放在集群中运行的。现在在本地运行，会导致很多配置文件确实。需要补充。例如从hivemetadata获取到了表路径webankcluster就不认识。
    val spark = SparkSession
      .builder()
      //.master("spark://192.168.56.103:7077")
      .master("local[*]")
      .appName("Example2")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.uris", "thrift://bdp-03:9083")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.addJar("/Users/qianbw/workspace-scala/spark-sql-poc/target/spark-sql-poc-1.0-SNAPSHOT.jar");

    import spark.implicits._
    val df = spark.sql("SELECT * FROM spark_test.sogoue").as[Record]
    //println(df.schema)
    //df.show()

    df.createTempView("sogoue")

    /************ GROUP, ORDER **************/
    //val result = spark.sql("SELECT search_word, count(*) as count FROM sogoue group by search_word order by count desc")
    //result.show()

    /************ distinct **************/
    //val result = spark.sql("SELECT count(distinct(search_word)) FROM sogoue")
    //result.show()

    //val result1 = spark.sql("SELECT count(1) FROM sogoue")
    //result1.show()

    /************ now **************/
    val result = spark.sql("SELECT *, now() FROM sogoue")
    result.show()

    spark.stop()
  }


  case class Record(search_word: String, url: String, search_kind: String)

}

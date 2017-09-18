/*
 * 没搞定qianbw这个用户访问非default数据库的问题。所以这里都使用default数据库
 */
package my.sql.hive.sogou

// $example on:spark_hive$
import org.apache.spark.sql.SparkSession
// $example off:spark_hive$

object Example1 {

  def main(args: Array[String]) {
    // When working with Hive, one must instantiate `SparkSession` with Hive support, including
    // connectivity to a persistent Hive metastore, support for Hive serdes, and Hive user-defined
    // functions. Users who do not have an existing Hive deployment can still enable Hive support.
    // When not configured by the hive-site.xml, the context automatically creates `metastore_db`
    // in the current directory and creates a directory configured by `spark.sql.warehouse.dir`,
    // which defaults to the directory `spark-warehouse` in the current directory that the spark
    // application is started.

    // $example on:spark_hive$
    // warehouseLocation points to the default location for managed databases and tables
    val warehouseLocation = "hdfs://bdp-02:8020/user/hive/warehouse/hadoop";

    System.setProperty("HADOOP_USER_NAME", "hadoop")
    //  driver本来是要放在集群中运行的。现在在本地运行，会导致很多配置文件确实。需要补充。例如从hivemetadata获取到了表路径webankcluster就不认识。
    val spark = SparkSession
      .builder()
      .master("spark://192.168.56.103:7077")
      .appName("Example1")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.uris", "thrift://bdp-03:9083")
      .enableHiveSupport()
      .getOrCreate()

    spark.sparkContext.addJar("/Users/qianbw/workspace-scala/spark-sql-poc/target/spark-sql-poc-1.0-SNAPSHOT.jar");

    import spark.implicits._
    val df = spark.sql("SELECT * FROM spark_test.sogoue").as[Record]
    println(df.schema)
    df.show()

    //df.where($"search_kind" === "2").orderBy($"url".asc).select($"search_word", $"url", $"search_kind").show()

    //df.where($"search_kind" === "1").orderBy($"url".asc).select($"search_word", $"url", $"search_kind").show()

    // udf

    // udaf

    spark.stop()
  }


  case class Record(search_word: String, url: String, search_kind: String)

}

/*
 * 没搞定qianbw这个用户访问非default数据库的问题。所以这里都使用default数据库
 */
package my.sql.hive

// $example on:spark_hive$
import java.io.File

import org.apache.spark.sql.{Row, SparkSession}
// $example off:spark_hive$

object SparkHiveStandalone {

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
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.metastore.uris", "thrift://bdp-03:9083")
      .enableHiveSupport()
      .getOrCreate()

    //spark.sparkContext.getConf.setJars(List("/Users/qianbw/workspace-scala/spark-sql-poc/out/artifacts/spark_sql_poc_jar/spark-sql-poc.jar"));
    //spark.sparkContext.addJar("/Users/qianbw/workspace-scala/spark-sql-poc/out/artifacts/spark_sql_poc_jar/spark-sql-poc.jar");
    spark.sparkContext.addJar("/Users/qianbw/workspace-scala/spark-sql-poc/target/spark-sql-poc-1.0-SNAPSHOT.jar");

    import spark.implicits._

    spark.sql("show databases").show()
    spark.sql("show tables").show()
    // Queries are expressed in HiveQL
    spark.sql("SELECT * FROM spark_test.src").show()
    // +---+-------+
    // |key|  value|
    // +---+-------+
    // |238|val_238|
    // | 86| val_86|
    // |311|val_311|
    // ...

    // Aggregation queries are also supported.
    spark.sql("SELECT COUNT(*) FROM spark_test.src").show()
    // +--------+
    // |count(1)|
    // +--------+
    // |    500 |
    // +--------+

    // The results of SQL queries are themselves DataFrames and support all normal functions.
    val sqlDF = spark.sql("SELECT key, value FROM spark_test.src WHERE key < 10 ORDER BY key")

    // The items in DataFrames are of type Row, which allows you to access each column by ordinal.
    val stringsDS = sqlDF.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }
    stringsDS.show()
    // +--------------------+
    // |               value|
    // +--------------------+
    // |Key: 0, Value: val_0|
    // |Key: 0, Value: val_0|
    // |Key: 0, Value: val_0|
    // ...

    // You can also use DataFrames to create temporary views within a SparkSession.
    val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    recordsDF.createOrReplaceTempView("records")

    // Queries can then join DataFrame data with data stored in Hive.
    spark.sql("SELECT * FROM records r JOIN spark_test.src s ON r.key = s.key").show()
    // +---+------+---+------+
    // |key| value|key| value|
    // +---+------+---+------+
    // |  2| val_2|  2| val_2|
    // |  4| val_4|  4| val_4|
    // |  5| val_5|  5| val_5|
    // ...
    // $example off:spark_hive$

    spark.stop()
  }

  // $example off:spark_hive$

  // $example on:spark_hive$
  case class Record(key: Int, value: String)
}

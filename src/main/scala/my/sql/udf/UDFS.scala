package my.sql

import java.text.SimpleDateFormat

object UDFS {

  /**
    * 格式化日期
    */
  val formatDate = (str: String) => {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val date = sdf.parse(str)
    sdf.format(date)
  }



  /**
    * 判断serchKind是否为2
    */
  val isSearchKind2 = (serchKind: String) => {
    var result = false;
    if (0 == serchKind.compareToIgnoreCase("2")) {
      result = true
    }
    else {
      result = false
    }
    result
  }
}

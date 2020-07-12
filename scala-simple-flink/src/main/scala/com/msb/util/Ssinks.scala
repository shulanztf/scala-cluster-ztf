package com.msb.util

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
 * @description: http://blog.itpub.net/31506529/viewspace-2636334/.
 * @author: zhaotf
 * @create: 2020-07-11 20:34
 */
class Ssinks extends RichSinkFunction[(String,String,Long)] {

  var conn: Connection = _;
  var pres: PreparedStatement = _;
  var username = "root";
  var password = "123456";
  var dburl = "jdbc:mysql://192.168.1.133:3306/hlhtpoint?useUnicode=true&characterEncoding=utf-8&useSSL=false";
  var sql = "insert into t_violation_list (car,violation,create_time) values (?,?,?)";

  override def invoke(value: (String,String,Long), context: SinkFunction.Context[_]): Unit = {
    pres.setString(1, value._1);
    pres.setString(2, value._2)
    pres.setLong(3, value._3);
    pres.executeUpdate();
    //    System.out.println("values ：" +value._1+"--"+value._2);
  }

  override def open(parameters: Configuration) {
    Class.forName("com.mysql.jdbc.Driver");
    conn = DriverManager.getConnection(dburl, username, password);
    pres = conn.prepareStatement(sql);
    super.close()
  }

  override def close() {
    pres.close();
    conn.close();
  }
}

package com.gaoqian

import java.sql.Connection

import com.jolbox.bonecp.{BoneCP, BoneCPConfig}
import org.slf4j.LoggerFactory

/**
  * mysql 连接池
  */

object ConnectionPool {

  val logger = LoggerFactory.getLogger(this.getClass)

  //连接池配置
  private val connectionPool: Option[BoneCP] = {
    try {
      Class.forName("com.mysql.jdbc.Driver")
      val config = new BoneCPConfig()
      config.setJdbcUrl("jdbc:mysql://localhost:3306/sparkmall")
      config.setUsername("root")
      config.setPassword("123456")
      config.setLazyInit(true)

      config.setMinConnectionsPerPartition(3)
      config.setMaxConnectionsPerPartition(5)
      config.setPartitionCount(5)
      config.setCloseConnectionWatch(true)
      config.setLogStatementsEnabled(false)
      Some(new BoneCP(config))
    } catch {
      case exception: Exception =>
        logger.warn("Create Connection Error: \n" + exception.printStackTrace())
        None
    }
  }

  // 获取数据库连接
  def getConnection: Option[Connection] = {
    connectionPool match {
      case Some(pool) => Some(pool.getConnection)
      case None => None
    }
  }

  // 释放数据库连接
  def closeConnection(connection: Connection): Unit = {
    if (!connection.isClosed) {
      connection.close()
    }
  }
}

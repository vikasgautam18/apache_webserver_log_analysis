package com.wordpress.technicado

import org.apache.spark.streaming.StreamingContext
import com.wordpress.technicado.commons.configuration.ConfigReader
import Constants._
import com.wordpress.technicado.commons.logging.Utils
import org.apache.spark.storage.StorageLevel

class ProcessWebLog(ssc: StreamingContext) {


  def process = {

    val logDStream = readLogFromSocket
    Utils.enableErrorLoggingOnly

    logDStream.foreachRDD(rdd => println(rdd.count))

    //TODO: work in progress

  }


  def readLogFromSocket = {
    ssc.socketTextStream(ConfigReader.getString(SOCKET_HOST_NAME),
      ConfigReader.getInt(SOCKET_PORT_NAME), StorageLevel.MEMORY_AND_DISK)
  }

}

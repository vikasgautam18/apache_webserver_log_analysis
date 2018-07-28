package com.wordpress.technicado.exports

import org.apache.spark.streaming.StreamingContext
import com.wordpress.technicado.commons.datagen._
import com.wordpress.technicado.commons.configuration._
import com.wordpress.technicado.Constants._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.{SQLContext, SparkSession}

class JSONProcess(ssc: StreamingContext) {
  def write(path: String) = {

    val socketDS = ssc.socketTextStream(
      ConfigReader.getString(SOCKET_HOST_NAME),
      ConfigReader.getInt(SOCKET_PORT_NAME),
      StorageLevel.MEMORY_AND_DISK
    )

    val logRecordDS: DStream[ApacheServerLogRecord] = socketDS.mapPartitions(iter => {
      val apacheServerLogParser = new ApacheServerLogParser()
      iter.flatMap(record => { //  to ignore all None(empty) records
          apacheServerLogParser.parseRecord(record)
      })
    })

    logRecordDS.foreachRDD(rdd => {

        val sqlContext = JSONProcess.getSQLContext
        import sqlContext.implicits._

        rdd.toDF().write.json(path)
    })
  }
}

object JSONProcess {

  def apply(ssc: StreamingContext): JSONProcess = new JSONProcess(ssc)

  def getSQLContext : SQLContext =
    SparkSession.builder().getOrCreate().sqlContext
}

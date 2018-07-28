package com.wordpress.technicado.exports

import com.wordpress.technicado.Constants._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.wordpress.technicado.commons.configuration.ConfigReader

object JsonFileOut {

  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      println("USAGE: spark-submit --class com.wordpresss.technicado.Main " +
        "--master local[*] spark/jars/apachewebserverloganalysis_2.11-0.1.jar hdfs://path/to/app.config")
      System.exit(-1)
    }

    val sc = new SparkContext(new SparkConf)
    ConfigReader.readConfig(args(0), sc)

    val ssc = StreamingContext.getOrCreate(ConfigReader.getString(JSON_CHECKPOINT_DIR), () => createContext(sc))

    ssc.start()
    ssc.awaitTermination()
  }

  def createContext(sc: SparkContext): StreamingContext = {

    val ssc =  new StreamingContext(sc, Seconds(ConfigReader.getLong(JSON_APP_DUR)))
    ssc.checkpoint(ConfigReader.getString(JSON_CHECKPOINT_DIR))
    JSONProcess(ssc).write(ConfigReader.getString(JSON_OUT_DIR))

    ssc
  }

}

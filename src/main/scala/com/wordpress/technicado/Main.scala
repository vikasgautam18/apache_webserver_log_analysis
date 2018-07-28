package com.wordpress.technicado

import com.wordpress.technicado.commons.configuration.ConfigReader
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
object Main {

  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("USAGE: spark-submit --class com.wordpresss.technicado.Main " +
        "--master local[*] spark/jars/apachewebserverloganalysis_2.11-0.1.jar hdfs://path/to/app.config")
      System.exit(-1)
    }


    val ssc = StreamingContext.getOrCreate(args(1), () => createContext(args))

    ssc.start()
    ssc.awaitTermination()

  }

  def createContext(args: Array[String]) = {
    val sc = new SparkContext(new SparkConf())
    ConfigReader.readConfig(args(0), sc)
    val ssc = new StreamingContext(sc, Seconds(ConfigReader.getInt(Constants.STREAMING_APP_DUR)))
    ssc.checkpoint(ConfigReader.getString(Constants.CHECKPOINT_DIR))

    new ProcessWebLog(ssc).process

    ssc
  }


}

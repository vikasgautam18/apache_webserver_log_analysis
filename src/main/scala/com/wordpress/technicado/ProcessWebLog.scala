package com.wordpress.technicado

import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.wordpress.technicado.commons.configuration.ConfigReader
import Constants._
import com.wordpress.technicado.commons.logging.Utils
import org.apache.spark.storage.StorageLevel
import java.util.regex._

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}


class ProcessWebLog(ssc: StreamingContext) {


  def process = {

    val logDStream: ReceiverInputDStream[String] = readLogFromSocket
    Utils.enableErrorLoggingOnly

    val pattern = patternMatcher
    val matched = logDStream.map(line => {val matcher: Matcher = pattern.matcher(line); if (matcher.matches()) matcher.group(5)})

    // Extract the URL from the request
    val urls = matched.map(x => {val arr = x.toString().split(" "); if (arr.size == 3) arr(1) else "[error]"})

    // Reduce by URL over a 5-minute window sliding every second
    val urlCounts = urls.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))

    // Sort and print the results
    val sortedResults = urlCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    sortedResults.print()
  }


  def readLogFromSocket = {
    ssc.socketTextStream(ConfigReader.getString(SOCKET_HOST_NAME),
      ConfigReader.getInt(SOCKET_PORT_NAME), StorageLevel.MEMORY_AND_DISK)
  }

  def patternMatcher = {
    import java.util.regex.Pattern
    val logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+) \"([^\"]+)\" \"([^\"]+)\""
    Pattern.compile(logEntryPattern)

  }

}

package com.xuzq.KafkaConsumer

import java.text.SimpleDateFormat
import java.util.concurrent._
import java.util.{Date, Properties}

import com.xuzq.Hdfs.HdfsAction
import com.xuzq.MD5.MessageMD5
import kafka.consumer.{Consumer, ConsumerConfig, KafkaStream}
import kafka.utils.Logging
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.matching.Regex
import scala.util.parsing.json.JSON

class KafkaConsumer(val zookeeper: String,
                    val groupId: String,
                    val topic: String,
                    val zpHbase: String,
                    val tbName: String,
                    val contentMinSize: String,
                    val regexFilePath: String,
                    val filePath: String) extends Logging {



  val config = createConsumerConfig(zookeeper, groupId)
  val consumer = Consumer.create(config)
  var executor: ExecutorService = null

  def shutdown() = {
    if (consumer != null)
      consumer.shutdown()
    if (executor != null)
      executor.shutdown()
  }

  def createConsumerConfig(zookeeper: String, groupId: String): ConsumerConfig = {
    val props = new Properties()
    props.put("zookeeper.connect", zookeeper)
    props.put("group.id", groupId)
    props.put("auto.offset.reset", "largest")
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    val config = new ConsumerConfig(props)
    config
  }

  def run(numThreads: Int) = {
    val topicCountMap = Map(topic -> numThreads)
    val consumerMap = consumer.createMessageStreams(topicCountMap)
    val streams = consumerMap.get(topic).get

    executor = Executors.newFixedThreadPool(numThreads)
    var threadNumber = 0
    for (stream <- streams) {
      executor.submit(new ScalaConsumerTest(stream, threadNumber, zpHbase, tbName, contentMinSize, regexFilePath, filePath))
      threadNumber += 1
    }
  }
}

object ScalaConsumerExample extends App {

  if (args.length < 9) {
    System.err.println("Usage: KafkaConsumer <zkQuorum> <group> <topic> <numThreads> <zkHbase> <tableName> <contentMinSize> <regexFilePath> <filePath>")
    System.exit(1)
  }
  /*val zkQuorum = "10.2.1.1:2181,10.2.1.2:2181,10.2.1.3:2181,10.2.1.4:2181/kafka"
  val group = "xuzq"
  val topic = "xuzq_sentiment"
  val numThreads = "3"
  val zkHbase = "mhdp1.b.xuzq.com,mhdp2.b.xuzq.com,hdp1.b.xuzq.com,hdp2.b.xuzq.com,hdp3.b.xuzq.com"
  val tableName = "public_opinion:metadata"
  val contentMinSize = "200"
  val filePath = "/home/spark/data/"*/

  val Array(zkQuorum, group, topic, numThreads, zkHbase, tableName, contentMinSize, regexFilePath, filePath) = args
  val example = new KafkaConsumer(zkQuorum, group, topic, zkHbase, tableName, contentMinSize, regexFilePath, filePath)
  example.run(numThreads.toInt)
}

class ScalaConsumerTest(val stream: KafkaStream[Array[Byte], Array[Byte]], val threadNumber: Int, val zpHbase: String, val tbName: String, val contentMinSize: String, val regexFilePath: String, val filePath: String) extends Logging with Runnable {
  def run {
    val regexforTitle = new StringBuilder()
    val linesList = Source.fromFile(regexFilePath).getLines.toList
    //val linesList = Source.fromFile("RegexforTitle.txt").getLines.toList

    for (line <- linesList){
      if(line != ""){
        if(line != linesList(linesList.size - 1)){
          regexforTitle.append(line + "|")
        }else{
          regexforTitle.append(line)
        }
      }
    }
    val patternForTitle = new Regex(regexforTitle.toString)
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val patternForUrl = new Regex("_\\d{1,2}.[a-z]+|_p\\d{1,2}.[a-z]+")
    val it = stream.iterator()
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zpHbase)
    val connection = ConnectionFactory.createConnection(conf)
    val userTable = TableName.valueOf(tbName)
    val table = connection.getTable(userTable)
    val hdfsAction = new HdfsAction()

    while (it.hasNext()) {
      val msg = new String(it.next().message())

      val fileName = new SimpleDateFormat("yyyy-MM-dd").format(new Date()) + ".txt"

      println(fileName + " -> " + msg)

      hdfsAction.writeContent(filePath + fileName, (msg.toString + "\n").getBytes)

      val jsonObj = JSON.parseFull(msg)
      val map: Map[String, Any] = jsonObj.get.asInstanceOf[Map[String, Any]]

      val urlFilterList = scala.collection.mutable.Set[String]()

      if((patternForUrl findAllIn map.get("url").get.toString).isEmpty){
        if(map.get("bodys").get.toString.size > contentMinSize.toInt){
          if(!urlFilterList.contains(map.get("url").get.toString)){
            urlFilterList.add(map.get("url").get.toString)
            val urlmd5 = new MessageMD5().messageMD5(map.get("url").get.toString)

            val rowKey = new ArrayBuffer[Byte]()
            rowKey ++= Bytes.toBytes(-new Date().getTime)
            rowKey ++= Bytes.toBytes(urlmd5)

            try {
              val p = new Put(rowKey.toArray)
              p.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("content"), Bytes.toBytes(map.get("bodys").get.toString))

              p.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("url"), Bytes.toBytes(map.get("url").get.toString))
              p.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("timestamp"), Bytes.toBytes(map.get("time").get.toString))

              var orgTitle = map.get("title").get.toString
              while(!(patternForTitle findAllIn orgTitle).isEmpty){
                orgTitle = patternForTitle replaceFirstIn(orgTitle, "")
              }

              p.addColumn(Bytes.toBytes("f2"), Bytes.toBytes("title"), Bytes.toBytes(orgTitle))

              table.put(p)

              println(df.format(new Date()) + ",Thread " + threadNumber + ": " + map.get("title").get.toString)
            } catch {
              case _: Exception => println(df.format(new Date()) + "raw error!")
            }
          }
        }
      }
    }
    println(df.format(new Date()) + "Shutting down Thread: " + threadNumber)
  }
}
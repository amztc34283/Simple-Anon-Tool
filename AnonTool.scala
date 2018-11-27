package src.main.scala

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object AnonymizeService {

  def initSpark(app:String="wer-privacy-test", master:String = "local[*]", awsKey:String="", awsSecret:String="", awsSession:String="",
                enableS3:Boolean=false, ui:String="false", parallelism:String="8"): SparkSession = {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(app)
      .set("spark.default.parallelism", parallelism)
      .set("spark.ui.enabled", ui)
      .set("fs.s3a.access.key", awsKey)
      .set("fs.s3a.secret.key", awsSecret)
    //The config is working.

    SparkSession.builder().config(conf).getOrCreate()
  }

  //TODO: Native Read CSV

  //TODO: Change return type to Option[Dataframe] to handle error
  def readFromMySQLDataFrame(sparkSession: SparkSession, tableName: String, databaseName: String, user: String, password: String): DataFrame = {
    var dataFrame: DataFrame = null
    try {
      val sparkContext: SparkContext = sparkSession.sparkContext
      dataFrame = new SQLContext(sparkContext)
        .read.format("jdbc")
        .option("url", "jdbc:mysql://" + databaseName) //database name
        .option("driver", "com.mysql.cj.jdbc.Driver") //com.mysql.cj.jdbc.Driver is the newer version, do not use com.mysql.jdbc.Driver
        .option("dbtable", tableName) //datatable name
        .option("user", user)
        .option("password", password)
        .load()
    } catch {
      case e: Exception => {
        println("Invalid Read from MySQL.")
        System.exit(1)
      }
    }
    dataFrame
  }

  def writeToMySQLDataFrame(tableName: String, databaseName: String, dataFrame: DataFrame, saveMode: SaveMode, user: String, password: String) = {
    try {
      dataFrame.write.format("jdbc")
        .option("url", "jdbc:mysql://" + databaseName)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", tableName)
        .option("user", user)
        .option("password", password)
        .mode(saveMode)
        .save()
    } catch {
      case e: Exception => {
        throw new Exception("Invalid Write to MySQL.")
      }
    }
  }

  def sha256Hash(text: String) : String =  {
    if (text == null) {
      return null
    }
    String.format("%064x", new java.math.BigInteger(1, java.security.MessageDigest.getInstance("SHA-256").digest(text.getBytes("UTF-8"))))
  }

  import Numeric.Implicits._

  //This function should expect input and output the same thing as long as you are putting scale and offset as Int type.
  //This function is using implicit.
  //https://stackoverflow.com/questions/43371466/scala-generic-function-multiplying-numerics-of-different-types
  //TODO: Check scale and offset are the type of Int
  def scaleAndOffset[T: Numeric](scale: T, offset: T, entry: T): T = {
    scale * (entry + offset)
  }

  //This function is using the function above, which means we need to pass in implicit when calling this function.
  //We let scale and offset to be T, but we will cast them to Int in scaleAndOffset.
  def scaleAndOffsetUDF[T: Numeric](scale: T, offset: T) {
    ((entry: T) => scaleAndOffset(scale, offset, entry))
  }

  case class AnonFields(name: String, dataType: String, scale: Int = 0, offset: Int = 0)
  case class Dictionary(lookupTable: DataFrame, dictionaryName: String) {
    def persist(destination: String): Unit = {
      //TODO: Check if there is duplicate before writing back to MySQL.
      //TODO: Go through the whole dictionary to check duplicate or make column unique
      //TODO: if column unique, and error got thrown, use salt table
      //TODO: doing the duplicate error check on the write level.
      //      writeToMySQLDataFrame()
    }
  }

  //  def handleDuplicate(dataFrame: DataFrame): DataFrame = {
  //
  //  }

  //By client or use case
  def lookup(sparkSession: SparkSession, tableName: String, databaseName: String = "localhost/test", userName: String = "root", password: String): Dictionary = {
    val lookupTable: DataFrame = readFromMySQLDataFrame(sparkSession, tableName, databaseName, userName, password)
    //read will fail if it is invalid lookup.
    new Dictionary(lookupTable, tableName)
  }

  //RT: Dictionary
  def anon(sparkSession: SparkSession, inputData: DataFrame, cols: Seq[AnonFields], dictionary: Option[Dictionary], newDictionaryName: String = "", writeBackFunction: (DataFrame) => Unit): Dictionary = {

    if (dictionary == None && newDictionaryName == "") {
      throw new Exception("Please specify the name of your new dictionary.")
    }
    val sqlContext = sparkSession.sqlContext
    import sqlContext.implicits._

    var outputDictionary: DataFrame = Seq.empty[(String, String)].toDF("key", "value")
    var anonyData: DataFrame = inputData
    //Implicit Cast
    val columnsName: Seq[String] = inputData.columns

    //TODO: Determine how to store scale and offset, maybe a new table.
    for (column <- cols) {
      column.dataType match {
        case "string" => {
          val hashUDF = udf[String, String](sha256Hash)
          //This will change the order.
          anonyData = anonyData
            .withColumn("something", hashUDF(anonyData.col(column.name)))

          outputDictionary = outputDictionary.union(anonyData
            .withColumn("key", anonyData.col("something"))
            .withColumn("value", anonyData.col(column.name))
            .select("key", "value"))

          anonyData = anonyData
            .drop(column.name)
            .withColumnRenamed("something", column.name)
        }
        case "long" => {
          def scaleAndOffsetUDF(scale: Int, offset: Int) = ((entry: Long) => scaleAndOffset(scale, offset, entry))
          val scaleAndOffsetRealUDF = udf[Long, Long](scaleAndOffsetUDF(column.scale, column.offset))
          anonyData = anonyData
            .withColumn("something", scaleAndOffsetRealUDF(anonyData.col(column.name)))
            .drop(column.name)
            .withColumnRenamed("something", column.name)
        }
        case "int" => {
          def scaleAndOffsetUDF(scale: Int, offset: Int) = ((entry: Int) => scaleAndOffset(scale, offset, entry))
          val scaleAndOffsetRealUDF = udf[Int, Int](scaleAndOffsetUDF(column.scale, column.offset))
          anonyData = anonyData
            .withColumn("something", scaleAndOffsetRealUDF(anonyData.col(column.name)))
            .drop(column.name)
            .withColumnRenamed("something", column.name)
        }
        case "float" => {
          def scaleAndOffsetUDF(scale: Int, offset: Int) = ((entry: Float) => scaleAndOffset(scale, offset, entry))
          val scaleAndOffsetRealUDF = udf[Float, Float](scaleAndOffsetUDF(column.scale, column.offset))
          anonyData = anonyData
            .withColumn("something", scaleAndOffsetRealUDF(anonyData.col(column.name)))
            .drop(column.name)
            .withColumnRenamed("something", column.name)
        }
        case "double" => {
          def scaleAndOffsetUDF(scale: Int, offset: Int) = ((entry: Double) => scaleAndOffset(scale, offset, entry))
          val scaleAndOffsetRealUDF = udf[Double, Double](scaleAndOffsetUDF(column.scale, column.offset))
          anonyData = anonyData
            .withColumn("something", scaleAndOffsetRealUDF(anonyData.col(column.name)))
            .drop(column.name)
            .withColumnRenamed("something", column.name)
        }
      }
    }

    //Preserve the order, added qweasdzxc as temp column to use the select properly.
    //columnsName has the original order of the columns
    anonyData = anonyData
      .withColumn("qweasdzxc", lit(""))
      .select("qweasdzxc", columnsName: _*)
      .drop("qweasdzxc")

    dictionary match {
      case Some(s) => {
        //TODO: Handle duplicates - via MySQL or Spark ?
        outputDictionary = s.lookupTable.union(outputDictionary)
        writeBackFunction(anonyData)
        return new Dictionary(outputDictionary, s.dictionaryName)
      }
      case None => {
        writeBackFunction(anonyData)
        return new Dictionary(outputDictionary, newDictionaryName)
      }
    }
  }

  //TODO: handle duplicate
  def denon(lookupTable: DataFrame, anonyData: DataFrame, cols: Seq[AnonFields]): DataFrame = {
    val columnName: Array[String] = anonyData.columns
    val leftTable = lookupTable.as("leftTable")
    var rightTable = anonyData.as("rightTable")
    for (column <- cols) {
      column.dataType match {
        case "string" => {
          rightTable = leftTable.join(rightTable, col("leftTable.key") === col("rightTable." + column.name)).select("leftTable.value", "rightTable.*").drop(column.name).withColumnRenamed("value", column.name)
        }
      }
    }
    val denonyWithOrderDataframe: DataFrame = rightTable.withColumn("qweasdzxc", lit(""))
      .select("qweasdzxc", columnName: _*)
      .drop("qweasdzxc")

    denonyWithOrderDataframe
  }

  def main(args: Array[String]): Unit = {
  }
}

package com.vesoft.tools

import java.io.File
import java.nio.charset.{Charset, UnsupportedCharsetException}

import com.vesoft.client.NativeClient
import org.apache.commons.cli.{
  CommandLine,
  DefaultParser,
  HelpFormatter,
  Options,
  ParseException,
  Option => CliOption
}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.io.BytesWritable
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.slf4j.LoggerFactory

/**
  * Use spark to generate sst files in batch, which will be ingested by Nebula engine.
  * $HADOOP_HOME env needs to be set.
  *
  * The following use cases are supported:
  *
  * <p>
  * Generate sst files from hive tables datasource, guided by a mapping file, which maps hive tables to vertexes and edges.
  * Multiple vertexes or edges may map to a single hive table, where a partition column will be used to distinguish different
  * vertex or edge.
  * The hive tables may periodically be regenerated by your business system to reflect the latest data so far, and   be
  * partitioned by a time column to indicate the generated time.
  * </p>
  */
object SparkSstFileGenerator {
  private[this] val log = LoggerFactory.getLogger(this.getClass)

  /**
    * configuration key for sst file output
    */
  val SSF_OUTPUT_LOCAL_DIR_CONF_KEY = "nebula.graph.spark.sst.file.local.dir"
  val SSF_OUTPUT_HDFS_DIR_CONF_KEY  = "nebula.graph.spark.sst.file.hdfs.dir"
  val NEBULA_PARTITION_NUMBER_KEY   = "nebula.graph.spark.sst.file.partition.number"

  /**
    * cmd line's options, whose name follow the convention: input options name end with 'i', while output end with 'o'"
    */
  lazy val options: Options = {
    val dataSourceTypeInput = CliOption
      .builder("ti")
      .longOpt("datasource_type_input")
      .hasArg()
      .desc("Data source types supported, must be among [hive|hbase|csv] for now, default=hive")
      .build

    val defaultColumnMapPolicy = CliOption
      .builder("ci")
      .longOpt("default_column_mapping_policy")
      .hasArg()
      .desc("If omitted, what policy to use when mapping column to property," +
        "all columns except primary_key's column will be mapped to tag's property with the same name by default")
      .build

    val mappingFileInput = CliOption
      .builder("mi")
      .longOpt("mapping_file_input")
      .required()
      .hasArg()
      .desc("Hive tables to nebula graph schema mapping file")
      .build

    val localSstFileOutput = CliOption
      .builder("so")
      .longOpt("local_sst_file_output")
      .required()
      .hasArg()
      .desc(
        "Which local directory those generated sst files will be put, should starts with file:///")
      .build

    val hdfsSstFileOutput = CliOption
      .builder("ho")
      .longOpt("hdfs_sst_file_output")
      .required()
      .hasArg()
      .desc("Which hdfs directory will those sstfiles be put, should not starts with file:///")
      .build

    val datePartitionKey = CliOption
      .builder("pi")
      .longOpt("date_partition_input")
      .required()
      .hasArg()
      .desc("A partition field of type String of hive table, which represent a Date, and has format of YYY-MM-dd")
      .build


    // when the newest data arrive, used in non-incremental environment
    val latestDate = CliOption
      .builder("di")
      .longOpt("latest_date_input")

      .required()
      .hasArg()
      .desc("Latest date to query,date format YYYY-MM-dd")
      .build

    val repartitionNumber = CliOption
      .builder("ri")
      .longOpt("repartition_number_input")
      .hasArg()
      .desc("Repartition number. Some optimization trick to improve generation speed and data skewness. Need tuning to suit your data.")
      .build


    // may be used in some test run to prove the correctness
    val limit = CliOption
      .builder("li")
      .longOpt("limit_input")

      .hasArg()
      .desc("Return at most this number of edges/vertex, usually used in POC stage, when omitted, fetch all data.")
      .build

    val charset = CliOption
      .builder("hi")
      .longOpt("string_value_charset_input")
      .hasArg()
      .desc("When the value is of type String,what charset is used when encoded,default to UTF-8")
      .build

    val opts = new Options()
    opts.addOption(defaultColumnMapPolicy)
    opts.addOption(dataSourceTypeInput)
    opts.addOption(mappingFileInput)
    opts.addOption(localSstFileOutput)
    opts.addOption(hdfsSstFileOutput)
    opts.addOption(datePartitionKey)
    opts.addOption(latestDate)
    opts.addOption(repartitionNumber)
    opts.addOption(charset)
    opts.addOption(limit)

  }

  // cmd line formatter when something is wrong with options
  lazy val formatter = {
    val format = new HelpFormatter
    format.setWidth(300)
    format
  }

  /**
    * composite key for vertex/edge RDD, the partitionId part is used by Partitioner,
    * the valueEncoded part is used by SortWithPartition
    *
    * @param partitionId partition number
    * @param id          vertex id OR edge start node id used by partition
    * @param `type`      tag/edge type
    * @param keyEncoded  vertex/edge key encoded by native client
    */
  case class GraphPartitionIdAndKeyValueEncoded(partitionId: Int,
                                                id: BigInt,
                                                `type`: Int,
                                                keyEncoded: BytesWritable)

  /**
    * Partition by the partitionId part of key
    */
  class SortByKeyPartitioner(num: Int) extends Partitioner {
    override def numPartitions: Int = num

    override def getPartition(key: Any): Int = {
      (key.asInstanceOf[GraphPartitionIdAndKeyValueEncoded].partitionId % numPartitions)
    }
  }

  val DefaultVersion = 1

  val random = scala.util.Random

  // default charset when encoding String type
  val DefaultCharset = "UTF-8"

  def main(args: Array[String]): Unit = {
    val parser = new DefaultParser

    var cmd: CommandLine = null
    try {
      cmd = parser.parse(options, args)
    } catch {
      case e: ParseException => {
        log.error("Illegal arguments", e)
        formatter.printHelp("nebula spark sst file generator", options)
        System.exit(-1)
      }
    }

    var dataSourceTypeInput: String = cmd.getOptionValue("ti")
    if (dataSourceTypeInput == null) {
      dataSourceTypeInput = "hive"
    }

    var columnMapPolicy: String = cmd.getOptionValue("ci")
    if (columnMapPolicy == null) {
      columnMapPolicy = "hash_primary_key"
    }

    val mappingFileInput: String   = cmd.getOptionValue("mi")
    var localSstFileOutput: String = cmd.getOptionValue("so")
    while (localSstFileOutput.endsWith("/")) {
      localSstFileOutput = localSstFileOutput.stripSuffix("/")
    }

    // make sure use local file system to write sst file
    if (!localSstFileOutput.toLowerCase.startsWith("file://")) {
      throw new IllegalArgumentException(
        "Argument: -so --local_sst_file_output should start with file:///")
    }

    // Clean up local temp dir
    FileUtils.forceDeleteOnExit(new File(localSstFileOutput))

    var hdfsSstFileOutput: String = cmd.getOptionValue("ho")
    while (hdfsSstFileOutput.endsWith("/")) {
      hdfsSstFileOutput = hdfsSstFileOutput.stripSuffix("/")
    }

    if (hdfsSstFileOutput.toLowerCase.startsWith("file:///")) {
      throw new IllegalArgumentException(
        "Argument: -ho --hdfs_sst_file_output should not start with file:///")
    }

    val limitOption: String = cmd.getOptionValue("li")
    val limit = if (limitOption != null && limitOption.nonEmpty) {
      try {
        s"LIMIT ${limitOption.toLong}"
      } catch {
        case _: NumberFormatException => ""
      }
    } else ""

    //when date partition is used, we should use the LATEST data
    val datePartitionKey: String = cmd.getOptionValue("pi")
    val latestDate               = cmd.getOptionValue("di")

    val repartitionNumberOpt = cmd.getOptionValue("ri")
    val repartitionNumber: Option[Int] =
      if (repartitionNumberOpt == null || repartitionNumberOpt.isEmpty) {
        None
      } else {
        try {
          Some(repartitionNumberOpt.toInt)
        } catch {
          case _: Exception => {
            log.error(
              s"Argument: -ri --repartition_number_input should be int, but found:${repartitionNumberOpt}")
            None
          }
        }
      }

    // to test whether charset is supported
    val charsetOpt = cmd.getOptionValue("hi")
    val charset =
      if (charsetOpt == null || charsetOpt.isEmpty) {
        DefaultCharset
      } else {
        try {
          try {
            Charset.forName(charsetOpt)
            charsetOpt
          } catch {
            case _: UnsupportedCharsetException => {
              log.error(
                s"Argument: -hi --string_value_charset_input is a not supported charset:${charsetOpt}")
              DefaultCharset
            }
          }
        }
      }

    // parse mapping file
    val mappingConfiguration: MappingConfiguration = MappingConfiguration(mappingFileInput)

    val sparkConf  = new SparkConf().setAppName("nebula-graph-sstFileGenerator")
    val sc         = new SparkContext(sparkConf)
    val sqlContext = new HiveContext(sc)

    // to pass sst file dir to SstFileOutputFormat
    sc.hadoopConfiguration.set(SSF_OUTPUT_LOCAL_DIR_CONF_KEY, localSstFileOutput)
    sc.hadoopConfiguration.set(SSF_OUTPUT_HDFS_DIR_CONF_KEY, hdfsSstFileOutput)
    sc.hadoopConfiguration
      .set(NEBULA_PARTITION_NUMBER_KEY, mappingConfiguration.partitions.toString)

    // disable file output compression, because rocksdb can't recognize it
    sc.hadoopConfiguration.set(FileOutputFormat.COMPRESS, "false")

    // id generator lambda, use FNV hash for now
    //TODO: support id generator function other than FNV hash

    //TODO: handle hash collision, might cause data corruption
    val idGeneratorFunction = mappingConfiguration.keyPolicy.map(_.toLowerCase) match {
      case Some("hash_primary_key") =>
        (key: String) =>
          FNVHash.hash64a(key.getBytes("UTF-8"))
      case Some(a @ _) => throw new IllegalStateException(s"Not supported key generator=${a}")
      case None =>
        (key: String) =>
          FNVHash.hash64a(key.getBytes("UTF-8"))
    }


    // implicit ordering used by PairedRDD.repartitionAndSortWithinPartitions whose key is PartitionIdAndBytesEncoded typed
    implicit def ordering[A <: GraphPartitionIdAndKeyValueEncoded]: Ordering[A] = new Ordering[A] {
      override def compare(x: A, y: A): Int = {
        x.keyEncoded.compareTo(y.keyEncoded)
      }
    }

    val partitionNumber = repartitionNumber.getOrElse(mappingConfiguration.partitions)

    //1) handle vertex, encode all column except PK column as a single Tag's properties
    mappingConfiguration.tags.zipWithIndex.foreach {
      //tag index used as tagType
      case (tag: Tag, tagType: Int) => {

        //all column w/o PK column, allColumns does not include primaryKey
        val (allColumns, _) = validateColumns(sqlContext,
                                              tag,
                                              Seq(tag.primaryKey),
                                              Seq(tag.primaryKey),
                                              mappingConfiguration.databaseName)
        val columnExpression = {
          if (allColumns.isEmpty) {
            log.warn(
              s"Tag:${tag.name} in database doesn't has any column, so three will be no property defined.")
          }

          allColumns.map(_.columnName).fold(tag.primaryKey) { (acc, column) =>
            acc + "," + column
          }
        }

        val whereClause = tag.typePartitionKey
          .map(key => s"${key}='${tag.name}' AND ${datePartitionKey}='${latestDate}'")
          .getOrElse(s"${datePartitionKey}='${latestDate}'")
        //TODO:to handle multiple partition columns' Cartesian product
        val sql =
          s"SELECT ${columnExpression} FROM ${mappingConfiguration.databaseName}.${tag.tableName} WHERE ${whereClause} ${limit}"
        val tagDF = sqlContext.sql(sql)
        //RDD[(businessKey->values)]
        val tagKeyAndValues = tagDF.map(row => {
          (row.getAs[String](tag.primaryKey) + "_" + tag.name, //businessId_tagName will be unique, and used as key before HASH
           allColumns
             .filter(!_.columnName.equalsIgnoreCase(tag.primaryKey))
             .map(valueExtractor(row, _, charset)))
        })

        val tagKeyAndValuesPersisted = tagKeyAndValues
          .map {
            case (key, values) => {
              val vertexId: BigInt = idGeneratorFunction.apply(key)
              // assert(vertexId > 0)
              // random id, used to evenly distribute data
              val randomId: Int = random.nextInt(partitionNumber)
              // actual graph partition
              val graphPartitionId = (vertexId % partitionNumber).toInt

              // use NativeClient to generate key and encode values
              // log.debug(s"vertexId=${vertexId}, Tag(partition=${graphPartitionId}): " + DatatypeConverter.printHexBinary(keyEncoded) + " = " + DatatypeConverter.printHexBinary(valuesEncoded))
              (GraphPartitionIdAndKeyValueEncoded(randomId,
                                                  vertexId,
                                                  tagType,
                                                  new BytesWritable(
                                                    NativeClient.createVertexKey(graphPartitionId,
                                                                                 vertexId.toLong,
                                                                                 tagType,
                                                                                 DefaultVersion))),
               new PropertyValueAndTypeWritable(
                 new BytesWritable(NativeClient.encode(values.toArray))))
            }

          }
          .repartition(partitionNumber)
          .sortByKey()
          .persist(StorageLevel.DISK_ONLY)

        tagKeyAndValuesPersisted.saveAsNewAPIHadoopFile(localSstFileOutput,
                                                        classOf[GraphPartitionIdAndKeyValueEncoded],
                                                        classOf[PropertyValueAndTypeWritable],
                                                        classOf[SstFileOutputFormat])

        tagKeyAndValuesPersisted.unpersist(true)
      }
    }

    // For now nebula doesn't support expanding through all edgeTypes(The wildcard in the following nGQL `go from src over * where $.prop1="pin2mac" yield src.id, dst.id`)
    // so we work around it: All edges are of same type, and given fixed names. Use edge name as an extra property to distinguish them.
    // TODO: when nebula supports the above features, we will undo those changes.
    //2)  handle edges
    mappingConfiguration.edges.zipWithIndex.foreach {
      //edge index used as edge_type
      case (edge: Edge, edgeType: Int) => {

        //all column w/o PK column
        val (allColumns, _) =
          validateColumns(sqlContext,
                          edge,
                          Seq(edge.fromForeignKeyColumn),
                          Seq(edge.fromForeignKeyColumn, edge.toForeignKeyColumn),
                          mappingConfiguration.databaseName)


        val columnExpression = {
          assert(allColumns.size > 0)
          s"${edge.fromForeignKeyColumn},${edge.toForeignKeyColumn}," + allColumns
            .map(_.columnName)
            .mkString(",")
        }

        val whereClause = edge.typePartitionKey
          .map(key => s"${key}='${edge.name}' AND ${datePartitionKey}='${latestDate}'")
          .getOrElse(s"${datePartitionKey}='${latestDate}'")


        //TODO: join FROM_COLUMN and join TO_COLUMN from the table where this columns referencing, to make sure that the claimed id really exists in the reference table.BUT with HUGE Perf penalty
        val edgeDf = sqlContext.sql(
          s"SELECT ${columnExpression} FROM ${mappingConfiguration.databaseName}.${edge.tableName} WHERE ${whereClause} ${limit}")
        //assert(edgeDf.count() > 0)
        val edgeKeyAndValues = edgeDf.map(row => {
          (row.getAs[String](edge.fromForeignKeyColumn) + "_" + edge.fromReferenceTag, // consistent with vertexId generation logic, to make sure that vertex and its' outbound edges are in the same partition
           row.getAs[String](edge.toForeignKeyColumn),
           allColumns
             .filterNot(col =>
               (col.columnName.equalsIgnoreCase(edge.fromForeignKeyColumn) || col.columnName
                 .equalsIgnoreCase(edge.toForeignKeyColumn)))
             .map(valueExtractor(row, _, charset)))
        })

        val edgeKeyAndValuesPersisted = edgeKeyAndValues
          .map {
            case (srcIDString, dstIdString, values) => {
              val id = idGeneratorFunction.apply(srcIDString)
              assert(id > 0)
              val graphPartitionId: Int = (id % partitionNumber).toInt

              val randomId: Int = random.nextInt(partitionNumber)

              val dstId = idGeneratorFunction.apply(dstIdString)

              // TODO: support edge ranking,like create_time desc
              //val keyEncoded = NativeClient.createEdgeKey(partitionId, srcId, edgeType, -1L, dstId, DefaultVersion)

              // TODO: only support a single edge type , put edge_type value in 0th index. Nebula server side must define extra edge property: edge_type
              //val valuesEncoded: Array[Byte] = NativeClient.encode(values.toArray)
              //log.debug(s"id=${id}, Edge(partition=${graphPartitionId}): " + DatatypeConverter.printHexBinary(keyEncoded) + " = " + DatatypeConverter.printHexBinary(valuesEncoded))
              (GraphPartitionIdAndKeyValueEncoded(randomId,
                                                  id,
                                                  1,
                                                  new BytesWritable(
                                                    NativeClient.createEdgeKey(graphPartitionId,
                                                                               id.toLong,
                                                                               1,
                                                                               -1L,
                                                                               dstId.toLong,
                                                                               DefaultVersion))),
               new PropertyValueAndTypeWritable(
                 new BytesWritable(
                   NativeClient.encode((edge.name.getBytes(charset) +: values).toArray)),
                 VertexOrEdgeEnum.Edge))
            }
          }
          .repartition(partitionNumber)
          .sortByKey()
          .persist(StorageLevel.DISK_ONLY)

        edgeKeyAndValuesPersisted.saveAsNewAPIHadoopFile(
          localSstFileOutput,
          classOf[GraphPartitionIdAndKeyValueEncoded],
          classOf[PropertyValueAndTypeWritable],
          classOf[SstFileOutputFormat])

        edgeKeyAndValuesPersisted.unpersist(true)
      }
    }
  }

  /**
    * extract value from a column
    */
  private def valueExtractor(row: Row, col: Column, charset: String) = {
    col.`type`.toUpperCase match {
      case "INTEGER" => Int.box(row.getAs[Int](col.columnName))
      case "STRING"  => row.getAs[String](col.columnName).getBytes(charset)
      case "FLOAT"   => Float.box(row.getAs[Float](col.columnName))
      case "LONG"    => Long.box(row.getAs[Long](col.columnName))
      case "DOUBLE"  => Double.box(row.getAs[Double](col.columnName))
      case "BOOL"    => Boolean.box(row.getAs[Boolean](col.columnName))
      case a @ _     => throw new IllegalStateException(s"Unsupported edge data type ${a}")
    }
  }

  /**
    * check the columns claimed in mapping configuration file are indeed defined in db(hive)
    * and their type is compatible, if not, throw exception, return all required column definitions
    *
    * @return Tuple2(AllColumns w/o partition columns, partition columns)
    */
  private def validateColumns(sqlContext: HiveContext,
                              edge: WithColumnMapping,
                              colsMustCheck: Seq[String],
                              colsMustFilter: Seq[String],
                              databaseName: String): (Seq[Column], Seq[String]) = {
    val descriptionDF = sqlContext.sql(s"DESC ${databaseName}.${edge.tableName}")
    // all columns' name ---> type mapping in db
    val allColumnsMapInDB: Seq[(String, String)] = descriptionDF
      .map {
        case Row(colName: String, colType: String, _) => {
          (colName.toUpperCase, colType.toUpperCase)
        }
      }
      .collect
      .toSeq

    // columns that generated by DESC are separated by comments, before comments are non-partition columns, after comments are partition columns
    val commentsStart = allColumnsMapInDB.indexWhere(_._1.startsWith("#"))
    val (allColumnMap, partitionColumns): (Map[String, String], Seq[(String, String)]) =
      if (commentsStart == -1) {
        (allColumnsMapInDB.toMap, Seq.empty[(String, String)])
      } else {
        val commentsEnd = allColumnsMapInDB.lastIndexWhere(_._1.startsWith("#"))
        assert((commentsEnd >= commentsStart) && ((commentsEnd + 1) < allColumnsMapInDB.size))
        // all columns except partition columns
        (allColumnsMapInDB.slice(0, commentsStart).toMap,
         allColumnsMapInDB.slice(commentsEnd + 1, allColumnsMapInDB.size))
      }


    // check the claimed columns really exist in db
    colsMustCheck.map(_.toUpperCase).foreach { col =>
      if (allColumnMap.get(col).isEmpty) {
        throw new IllegalStateException(
          s"${edge.name}'s from column: ${col} not defined in table=${edge.tableName}")
      }

    }

    if (edge.columnMappings.isEmpty) {
      //only (from,to) columns are checked, but all columns should be returned
      (allColumnMap
         .filter(!partitionColumns.contains(_))
         .filter(!colsMustFilter.contains(_))
         .map {
           case (colName, colType) => {
             Column(colName, colName, colType) // propertyName default=colName
           }
         }
         .toSeq,
       partitionColumns.map(_._1))
    } else {

      // tag/edge's columnMappings should be checked and returned
      val columnMappings = edge.columnMappings.get
      val notValid = columnMappings
        .filter(
          col => {
            val typeInDb = allColumnMap.get(col.columnName.toUpperCase)
            typeInDb.isEmpty || !DataTypeCompatibility.isCompatible(col.`type`, typeInDb.get)
          }
        )
        .map {
          case col => s"name=${col.columnName},type=${col.`type`}"
        }

      if (notValid.nonEmpty) {
        throw new IllegalStateException(
          s"${edge.name}'s columns: ${notValid.mkString("\t")} not defined in or compatible with db's definitions")
      } else {
        (columnMappings, partitionColumns.map(_._1))
      }
    }
  }
}

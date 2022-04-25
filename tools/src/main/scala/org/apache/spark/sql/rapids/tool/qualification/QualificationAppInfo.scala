/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.rapids.tool.qualification

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap}

import com.nvidia.spark.rapids.tool.EventLogInfo
import com.nvidia.spark.rapids.tool.profiling._
import com.nvidia.spark.rapids.tool.qualification._
import org.apache.hadoop.conf.Configuration

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.{SparkPlanGraph, SparkPlanGraphCluster, SparkPlanGraphNode}
import org.apache.spark.sql.rapids.tool.{AppBase, ToolUtils}

class QualificationAppInfo(
    eventLogInfo: Option[EventLogInfo],
    hadoopConf: Option[Configuration] = None,
    pluginTypeChecker: Option[PluginTypeChecker],
    readScorePercent: Int)
  extends AppBase(eventLogInfo, hadoopConf) with Logging {

  // Determine input column types from read
  // Some could be partitioned data so would have to try to infer.. but what types are supported:
  //   partition data types have to be atomic
  //   https://github.com/apache/spark/blob/master/sql/core
  //   /src/main/scala/org/apache/spark/sql/execution/datasources/PartitioningUtils.scala#L559
  //



  var appId: String = ""
  var isPluginEnabled = false
  var lastJobEndTime: Option[Long] = None
  var aggJobTime: Option[Long] = None
  var lastSQLEndTime: Option[Long] = None
  val writeDataFormat: ArrayBuffer[String] = ArrayBuffer[String]()

  // jobId to job info
  val jobIdToInfo = new HashMap[Int, JobInfoClass]()

  var appInfo: Option[QualApplicationInfo] = None
  val sqlStart: HashMap[Long, QualSQLExecutionInfo] = HashMap[Long, QualSQLExecutionInfo]()

  // The duration of the SQL execution, in ms.
  val sqlDurationTime: HashMap[Long, Long] = HashMap.empty[Long, Long]

  val sqlIDToTaskEndSum: HashMap[Long, StageTaskQualificationSummary] =
    HashMap.empty[Long, StageTaskQualificationSummary]

  val stageIdToTaskEndSum: HashMap[Long, StageTaskQualificationSummary] =
    HashMap.empty[Long, StageTaskQualificationSummary]

  val stageIdToSqlID: HashMap[Int, Long] = HashMap.empty[Int, Long]
  val jobIdToSqlID: HashMap[Int, Long] = HashMap.empty[Int, Long]
  val sqlIDtoJobFailures: HashMap[Long, ArrayBuffer[Int]] = HashMap.empty[Long, ArrayBuffer[Int]]

  val notSupportFormatAndTypes: HashMap[String, Set[String]] = HashMap[String, Set[String]]()
  var wholeStage: ArrayBuffer[WholeStageCodeGenResults] = ArrayBuffer[WholeStageCodeGenResults]()
  // accum id to task stage accum info
  var taskStageAccumMap: mutable.HashMap[Long, ArrayBuffer[TaskStageAccumCase]] =
    mutable.HashMap[Long, ArrayBuffer[TaskStageAccumCase]]()
  // val accumIdToStageId: mutable.HashMap[Long, Int] = new mutable.HashMap[Long, Int]()
  var sqlPlan: mutable.HashMap[Long, SparkPlanInfo] = mutable.HashMap.empty[Long, SparkPlanInfo]

  private lazy val eventProcessor =  new QualificationEventProcessor(this)

  /**
   * Get the event listener the qualification tool uses to process Spark events.
   * Install this listener in Spark.
   *
   * {{{
   *   spark.sparkContext.addSparkListener(listener)
   * }}}
   * @return SparkListener
   */
  def getEventListener: SparkListener = {
    eventProcessor
  }

  processEvents()

  override def processEvent(event: SparkListenerEvent): Boolean = {
    eventProcessor.processAnyEvent(event)
    false
  }

  // time in ms
  private def calculateAppDuration(startTime: Long): Option[Long] = {
    if (startTime > 0) {
      val estimatedResult =
        this.appEndTime match {
          case Some(t) => this.appEndTime
          case None =>
            if (lastSQLEndTime.isEmpty && lastJobEndTime.isEmpty) {
              None
            } else {
              logWarning(s"Application End Time is unknown for $appId, estimating based on" +
                " job and sql end times!")
              // estimate the app end with job or sql end times
              val sqlEndTime = if (this.lastSQLEndTime.isEmpty) 0L else this.lastSQLEndTime.get
              val jobEndTime = if (this.lastJobEndTime.isEmpty) 0L else lastJobEndTime.get
              val maxEndTime = math.max(sqlEndTime, jobEndTime)
              if (maxEndTime == 0) None else Some(maxEndTime)
            }
        }
      ProfileUtils.OptionLongMinusLong(estimatedResult, startTime)
    } else {
      None
    }
  }

  /**
   * The score starts out based on the over all task time spent in SQL dataframe
   * operations and then can only decrease from there based on if it has operations not
   * supported by the plugin.
   */
  private def calculateScore(readScoreRatio: Double, sqlDataframeTaskDuration: Long): Double = {
    // the readScorePercent is an integer representation of percent
    val ratioForReadScore = readScorePercent / 100.0
    val ratioForRestOfScore = 1.0 - ratioForReadScore
    // get the part of the duration that will apply to the read score
    val partForReadScore = sqlDataframeTaskDuration * ratioForReadScore
    // calculate the score for the read part based on the read format score
    val readScore = partForReadScore * readScoreRatio
    // get the rest of the duration that doesn't apply to the read score
    val scoreRestPart = sqlDataframeTaskDuration * ratioForRestOfScore
    scoreRestPart + readScore
  }

  // if the SQL contains a dataset, then duration for it is 0
  // for the SQL dataframe duration
  private def calculateSqlDataframeDuration: Long = {
    sqlDurationTime.foreach { case (k, v) =>
      logWarning(s"k $k v: $v")
    }
    sqlIDToDataSetOrRDDCase.foreach { case k =>
      logWarning(s"k $k")
    }
    sqlDurationTime.filterNot { case (sqlID, dur) =>
      sqlIDToDataSetOrRDDCase.contains(sqlID) || dur == -1
    }.values.sum
  }


  // The total task time for all tasks that ran during SQL dataframe
  // operations.  if the SQL contains a dataset, it isn't counted.
  private def calculateTaskDataframeDuration: Long = {
    val validSums = sqlIDToTaskEndSum.filterNot { case (sqlID, _) =>
      sqlIDToDataSetOrRDDCase.contains(sqlID) || sqlDurationTime.getOrElse(sqlID, -1) == -1
    }
    validSums.values.map(dur => dur.totalTaskDuration).sum
  }

  // Look at the total task times for all jobs/stages that aren't SQL or
  // SQL but dataset or rdd
  private def calculateNonSQLTaskDataframeDuration: Long = {
    val allTaskTime = stageIdToTaskEndSum.values.map(_.totalTaskDuration).sum

    val validSums = sqlIDToTaskEndSum.filter { case (sqlID, _) =>
      sqlIDToDataSetOrRDDCase.contains(sqlID) || sqlDurationTime.getOrElse(sqlID, -1) == -1
    }
    val taskTimeDataSetOrRDD = validSums.values.map(dur => dur.totalTaskDuration).sum
    // TODO make more efficient
    allTaskTime - taskTimeDataSetOrRDD - calculateTaskDataframeDuration
  }

  // assume overhead time is app time minus the job time.
  // TODO - What about shell where idle?
  private def calculateOverHeadTime(startTime: Long): Long = {
    val appTime = calculateAppDuration(startTime)
    appTime.map(_ - aggJobTime.getOrElse(0L)).getOrElse(0L)
  }

  private def getSQLDurationProblematic: Long = {
    probNotDataset.keys.map { sqlId =>
      sqlDurationTime.getOrElse(sqlId, 0L)
    }.sum
  }

  private def calculateCpuTimePercent: Double = {
    val validSums = sqlIDToTaskEndSum.filterNot { case (sqlID, _) =>
      sqlIDToDataSetOrRDDCase.contains(sqlID) || sqlDurationTime.getOrElse(sqlID, -1) == -1
    }
    val totalCpuTime = validSums.values.map { dur =>
      dur.executorCPUTime
    }.sum
    val totalRunTime = validSums.values.map { dur =>
      dur.executorRunTime
    }.sum
    ToolUtils.calculateDurationPercent(totalCpuTime, totalRunTime)
  }

  private def getAllReadFileFormats: String = {
    dataSourceInfo.map { ds =>
      s"${ds.format.toLowerCase()}[${ds.schema}]"
    }.mkString(":")
  }

  // For the read score we look at all the read formats and datatypes for each
  // format and for each read give it a value 0.0 - 1.0 depending on whether
  // the format is supported and if the data types are supported. We then sum
  // those together and divide by the total number.  So if none of the data types
  // are supported, the score would be 0.0 and if all formats and datatypes are
  // supported the score would be 1.0.
  private def calculateReadScoreRatio(): Double = {
    pluginTypeChecker.map { checker =>
      if (dataSourceInfo.size == 0) {
        1.0
      } else {
        val readFormatSum = dataSourceInfo.map { ds =>
          val (readScore, nsTypes) = checker.scoreReadDataTypes(ds.format, ds.schema)
          if (nsTypes.nonEmpty) {
            val currentFormat = notSupportFormatAndTypes.get(ds.format).getOrElse(Set.empty[String])
            notSupportFormatAndTypes(ds.format) = (currentFormat ++ nsTypes)
          }
          readScore
        }.sum
        readFormatSum / dataSourceInfo.size
      }
    }.getOrElse(1.0)
  }

  /**
   * Aggregate and process the application after reading the events.
   * @return Option of QualificationSummaryInfo, Some if we were able to process the application
   *         otherwise None.
   */
  def aggregateStats: Option[QualificationSummaryInfo] = {
    appInfo.map { info =>
      val appDuration = calculateAppDuration(info.startTime).getOrElse(0L)
      val sqlDataframeDur = calculateSqlDataframeDuration
      // wall clock time
      val executorCpuTimePercent = calculateCpuTimePercent
      val endDurationEstimated = this.appEndTime.isEmpty && appDuration > 0
      val sqlDurProblem = getSQLDurationProblematic
      val readScoreRatio = calculateReadScoreRatio

      val sqlDataframeTaskDuration = calculateTaskDataframeDuration
      val noSQLDataframeTaskDuration = calculateNonSQLTaskDataframeDuration
      val overheadTime = calculateOverHeadTime(info.startTime)
      val nonSQLDuration = noSQLDataframeTaskDuration + overheadTime
      val readScoreHumanPercent = 100 * readScoreRatio
      val readScoreHumanPercentRounded = f"${readScoreHumanPercent}%1.2f".toDouble
      val score = calculateScore(readScoreRatio, sqlDataframeTaskDuration)
      val scoreRounded = f"${score}%1.2f".toDouble
      val failedIds = sqlIDtoJobFailures.filter { case (_, v) =>
        v.size > 0
      }.keys.mkString(",")
      val notSupportFormatAndTypesString = notSupportFormatAndTypes.map { case(format, types) =>
        val typeString = types.mkString(":").replace(",", ":")
        s"${format}[$typeString]"
      }.mkString(";")
      val writeFormat = writeFormatNotSupported(writeDataFormat)
      val (allComplexTypes, nestedComplexTypes) = reportComplexTypes
      val problems = getAllPotentialProblems(getPotentialProblemsForDf, nestedComplexTypes)

      // TODO calculate the unsupported operator task duration, going to very hard
      // gpuUnsupportedSQLTaskDuration = ???
      val unsupportedDuration = 0L
      val speedupDuration = sqlDataframeTaskDuration - unsupportedDuration

      // TODO calculate speedup_factor - which is average of operator factors???
      val speedupFactor = 1.0
      val estimatedDuration = (speedupDuration/speedupFactor) + unsupportedDuration + nonSQLDuration
      logWarning(s"estimated duration is: $estimatedDuration")
      logWarning(s"speedupDur/factor duration is: ${speedupDuration/speedupFactor}")

      val appTaskDuration = nonSQLDuration + sqlDataframeTaskDuration
      logWarning(s"noon sql dur is: $nonSQLDuration sql" +
        s" dataframe task dur is $sqlDataframeTaskDuration")
      logWarning(s"appTaskDuration is: $appTaskDuration")
      val totalSpeedup = appTaskDuration / estimatedDuration
      logWarning(s"total speedup : $totalSpeedup")
      // recommendation
      val speedupBucket = if (totalSpeedup > 3) {
        "GREEN"
      } else if (totalSpeedup > 1.25) {
        "YELLOW"
      } else {
        "RED"
      }
      new QualificationSummaryInfo(info.appName, appId, scoreRounded, problems,
        sqlDataframeDur, sqlDataframeTaskDuration, nonSQLDuration,
        appDuration, executorCpuTimePercent,
        endDurationEstimated, sqlDurProblem, failedIds, readScorePercent,
        readScoreHumanPercentRounded, notSupportFormatAndTypesString,
        getAllReadFileFormats, writeFormat, allComplexTypes, nestedComplexTypes,
        estimatedDuration, unsupportedDuration, speedupDuration, speedupFactor,
        totalSpeedup, speedupBucket)
    }
  }
/*
  private def parseSingleExpression(exprStr: String): Unit = {
    // val Pattern = """\(.*\)""".r
    val Pattern =
     """(?=\()(?=((?:(?=.*?\((?!.*?\2)(.*\)(?!.*\3).*))(?=.*?\)(?!.*?\3)(.*)).)+?.*?(?=\2)[^(]*(?=\3$)))""".r

    val Pattern = """(?=\()(?:(?=.*?\((?!.*?\1)(.*\)(?!.*\2).*))(?=.*?\)(?!.*?\2)(.*)).)+?.*?(?=\1)[^(]*(?=\2$)""".r

    exprStr match {
      case Pattern(c) => println(c)
      case _ =>
    }
  }

 */
  private def parseExpression(exprStr: String): Unit = {

    // Filter ((isnotnull(s_state#688) AND (s_state#688 = TN)) AND isnotnull(s_store_sk#664))

    // can we split on AND/OR/NOT
    val exprSepAND = if (exprStr.contains("AND")) {
      exprStr.split(" AND ").map(_.trim)
    } else {
      Array(exprStr)
    }
    val exprSplit = if (exprStr.contains(" OR ")) {
      exprSepAND.flatMap(_.split(" OR ").map(_.trim))
    } else {
      exprSepAND
    }
    // ((isnotnull(s_state#688)
    // (s_state#688 = TN))
    // isnotnull(s_store_sk#664))
    val paranRemoved = exprSplit.map(_.replaceAll("""^\(+""", "").replaceAll("""\)\)$""", ")"))
    // isnotnull(s_state#688)
    // s_state#688 = TN)
    // isnotnull(s_store_sk#664)

    paranRemoved.foreach { case expr =>
      if (expr.contains(" ")) {
        // likely some conditional expression
        // TODO - add in artichmetic stuff (- / * )
        // TODO - what about years and literals?
        // TODO do we need to match on more then words and #?
        val pattern = """([\w#]+) ([+=<>|]+) ([\w#]+)""".r
        pattern.findFirstMatchIn(expr) match {
          case Some(func) =>
            logWarning(s" found expr: $func")
            if (func.groupCount < 3) {
              logError("found expr but its not the entire thing, not sure what is going on")
            }
            val first = func.group(1)
            val predicate = func.group(2)
            val second = func.group(3)
            // check for variable
            if (first.contains("#") || second.contains("#")) {
              logWarning(s"expr contains # $first or $second")

            } // else if ???
            val predStr = predicate match {
              case "=" => "EqualTo"
              case "<=>" => "EqualNullSafe"
              case "<" => "LessThan"
              case ">" => "GreaterThan"
              case "<=" => "LessThanOrEqual"
              case ">=" => "GreaterThanOrEqual"
            }
            logWarning(s"predicate string is $predStr")
            // TODO - lookup function name
          case None => logWarning("not sure what this is")
        }

      } else {
        // likely some function call
        val pattern = """(\w+)\(.*\)""".r
        pattern.findFirstMatchIn(expr) match {
          case Some(func) =>
            logWarning(s" found func: $func")
            if (expr.length != func.group(0).length || func.groupCount == 0) {
              logError("found function but its not the entire thing, not sure what is going on")
            }
            val funcName = func.group(1)
            // TODO - lookup function name
          case None => logWarning("not sure what this is")
        }

      }
    }

    /*
    val tempStringBuilder = new StringBuilder()
    val individualExprs: ArrayBuffer[String] = new ArrayBuffer()
    var angleBracketsCount = 0
    var parenthesesCount = 0
    var parenAtBeginning = false
    var previousChar: Option[Char] = None
    for ((char, index) <- exprStr.zipWithIndex) {
      char match {
        case '<' => angleBracketsCount += 1
        case '>' => angleBracketsCount -= 1
        // If the schema has decimals, Example decimal(6,2) then we have to make sure it has both
        // opening and closing parentheses(unless the string is incomplete due to V2 reader).
        case '(' =>
          if (index == 0) {
            parenAtBeginning = true
          }
          if (previousChar.nonEmpty && (previousChar != ' ' && previousChar != '(' && index != 0)) {
            // this should be as part of an expression
          } else {
            // this should be as part of a grouping
          }
          parenthesesCount += 1
        case ')' =>
          parenthesesCount -= 1

        case ' ' =>
          // end of a expr, is this true or can have space in side expr where 2 parameters?
          // func(one, two)
          logWarning(" found space")
        case _ =>
      }
      if (angleBracketsCount == 0 && parenthesesCount == 0 && char.equals(' ')) {
        individualExprs += tempStringBuilder.toString
        tempStringBuilder.setLength(0)
      } else {
        tempStringBuilder.append(char);
      }
      previousChar = Some(char)
    }

     */
  }

  // FilterExec(condition: Expression, child: SparkPlan)
  // Filter ((isnotnull(s_state#688) AND (s_state#688 = TN)) AND isnotnull(s_store_sk#664))
  private def processFilterExec(node: SparkPlanGraphNode): Unit = {
    val expr = node.desc.replaceFirst("Filter ", "")
    parseExpression(expr)
  }

  private def processUnknownExec(node: SparkPlanGraphNode): Unit = {
    // assume its something we don't support
  }

  def processSQLPlanForNodeTiming: Unit = {
    sqlPlan.foreach { case (sqlID, planInfo) =>
      val planGraph = SparkPlanGraph(planInfo)
      val allnodes = planGraph.allNodes
      for (node <- allnodes) {

        if (node.isInstanceOf[SparkPlanGraphCluster]) {
          val ch = node.asInstanceOf[SparkPlanGraphCluster].nodes
          ch.foreach { c =>
            wholeStage += WholeStageCodeGenResults(0, sqlID, node.id, node.name, c.name)
          }
          logWarning(s"graph node ${node.name} desc: ${node.desc} id: " +
            s"${node.id} children graph cluster: ${ch.map(_.name).mkString(",")}")

        } else {
          logWarning(s"graph node ${node.name} desc: ${node.desc} id: ${node.id}")
        }

        node match {
          case f if (f.name.contains("Filter")) =>
            processFilterExec(f)
          case w if (w.name.contains("WholeStageCodegen")) =>
            // TODO - does metrics for time have previous ops?  per op thing
            val accumId = w.metrics.find(_.name == "duration").map(_.accumulatorId)
            // TODO - can't get metric values until after parsing plan done for task metrics
            val taskForAccum = accumId.flatMap(id => taskStageAccumMap.get(id))
              .getOrElse(ArrayBuffer.empty)
            taskForAccum.foreach(a => logWarning(s"task accum value ${a.value}"))

            logWarning(s"WholeStageCodegen time took: ${w.metrics.toString()}")
          // processFilterExec(f)
          case o =>
            logWarning(s"node match other: ${o.name}")
        }
      }
    }
  }

  private[qualification] def processSQLPlan(sqlID: Long, planInfo: SparkPlanInfo): Unit = {
    checkMetadataForReadSchema(sqlID, planInfo)
    val planGraph = SparkPlanGraph(planInfo)
    val allnodes = planGraph.allNodes
    for (node <- allnodes) {
      // TODO - likely can combine some code below with some of the above matching
      checkGraphNodeForReads(sqlID, node)
      if (isDataSetOrRDDPlan(node.desc)) {
        sqlIDToDataSetOrRDDCase += sqlID
      }
      val issues = findPotentialIssues(node.desc)
      if (issues.nonEmpty) {
        val existingIssues = sqlIDtoProblematic.getOrElse(sqlID, Set.empty[String])
        sqlIDtoProblematic(sqlID) = existingIssues ++ issues
      }
      // Get the write data format
      if (node.name.contains("InsertIntoHadoopFsRelationCommand")) {
        val writeFormat = node.desc.split(",")(2)
        writeDataFormat += writeFormat
      }
    }
  }

  private def writeFormatNotSupported(writeFormat: ArrayBuffer[String]): String = {
    // Filter unsupported write data format
    val unSupportedWriteFormat = pluginTypeChecker.map { checker =>
      checker.isWriteFormatsupported(writeFormat)
    }.getOrElse(ArrayBuffer[String]())

    unSupportedWriteFormat.distinct.mkString(";").toUpperCase
  }
}

class StageTaskQualificationSummary(
    val stageId: Int,
    val stageAttemptId: Int,
    var executorRunTime: Long,
    var executorCPUTime: Long,
    var totalTaskDuration: Long)

case class QualApplicationInfo(
    appName: String,
    appId: Option[String],
    startTime: Long,
    sparkUser: String,
    endTime: Option[Long], // time in ms
    duration: Option[Long],
    endDurationEstimated: Boolean)

case class QualSQLExecutionInfo(
    sqlID: Long,
    startTime: Long,
    endTime: Option[Long],
    duration: Option[Long],
    durationStr: String,
    sqlQualDuration: Option[Long],
    hasDataset: Boolean,
    problematic: String = "")

case class QualificationSummaryInfo(
    appName: String,
    appId: String,
    score: Double,
    potentialProblems: String,
    sqlDataFrameDuration: Long,
    sqlDataframeTaskDuration: Long,
    nonSqlTaskDurationAndOverhead: Long,
    appDuration: Long,
    executorCpuTimePercent: Double,
    endDurationEstimated: Boolean,
    sqlDurationForProblematic: Long,
    failedSQLIds: String,
    readScorePercent: Int,
    readFileFormatScore: Double,
    readFileFormatAndTypesNotSupported: String,
    readFileFormats: String,
    writeDataFormat: String,
    complexTypes: String,
    nestedComplexTypes: String,
    estimatedDuration: Double,
    unsupportedDuration: Long,
    speedupDuration: Long,
    speedupFactor: Double,
    totalSpeedup: Double,
    speedupBucket: String)

object QualificationAppInfo extends Logging {
  def createApp(
      path: EventLogInfo,
      hadoopConf: Configuration,
      pluginTypeChecker: Option[PluginTypeChecker],
      readScorePercent: Int): Option[QualificationAppInfo] = {
    val app = try {
        val app = new QualificationAppInfo(Some(path), Some(hadoopConf), pluginTypeChecker,
          readScorePercent)
        logInfo(s"${path.eventLog.toString} has App: ${app.appId}")
        Some(app)
      } catch {
        case json: com.fasterxml.jackson.core.JsonParseException =>
          logWarning(s"Error parsing JSON: ${path.eventLog.toString}")
          None
        case il: IllegalArgumentException =>
          logWarning(s"Error parsing file: ${path.eventLog.toString}", il)
          None
        case e: Exception =>
          // catch all exceptions and skip that file
          logWarning(s"Got unexpected exception processing file: ${path.eventLog.toString}", e)
          None
      }
    app
  }
}

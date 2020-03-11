import java.io.FileWriter
import java.text.SimpleDateFormat
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object Milestone1 {

  // Patterns that are needed to extract the required information from the log file.
  private val appPattern = """application_1580812675067_[0-9][0-9][0-9][0-9]""".r.unanchored
  private val attemptPattern = """appattempt_1580812675067_[0-9][0-9][0-9][0-9]_[0-9][0-9][0-9][0-9][0-9][0-9]""".r.unanchored
  private val attemptStatusPattern = """FINAL_SAVING to \b(FINISHING|FAILED|KILLED)\b""".r.unanchored
  private val containerPattern = """container_e02_1580812675067_[0-9][0-9][0-9][0-9]_[0-9][0-9]_[0-9][0-9][0-9][0-9][0-9][0-9]""".r.unanchored
  private val ep2AppPattern = """application_1580812675067_01(2[1-9]|30)""".r.unanchored
  private val hostPattern = """> on host .*epfl.ch:""".r.unanchored
  private val timestampAppIdPattern = """_1580812675067_[0-9][0-9][0-9][0-9]""".r.unanchored
  private val usernamePattern = """user: .*,""".r.unanchored

  /**
   * Maps epoch2 app. IDs to the lines containing them.
   *
   * Takes a line String from th log file.
   * If the line contains an app. ID, it returns a tuple of the form (app ID, line).
   * If the line does not contain an app. ID, it returns  a tuple of the form ("", line).
   *
   * @param logLine the line from the log file
   * @return tuple (app. ID, line)
   */
  private def mapAppToLines(logLine: String): (String, String) = {

    if (appPattern.findFirstIn(logLine).isDefined)
      (appPattern.findFirstIn(logLine).getOrElse(""), logLine)
    else
      ("", logLine)
  }

  /**
   * Maps epoch2 application IDs to the users that started them.
   *
   * Takes a tuple of the form (app ID, line).
   * If the line contains the user who started the app, it returns a tuple of the form (app ID, user).
   * If the line does not contain an user, it returns  a tuple of the form (app. ID, "").
   *
   * @param appIdAndLogLine tuple (appId, line containing appId)
   * @return tuple (app. ID, line)
   */
  private def mapAppToUser(appIdAndLogLine: (String, String)):  (String, String) = {

    if(usernamePattern.findFirstIn(appIdAndLogLine._2).isDefined)
      (appIdAndLogLine._1, usernamePattern.findFirstIn(appIdAndLogLine._2).getOrElse("").stripSuffix(",").trim.split(" ")(1))
    else
      (appIdAndLogLine._1, "")
  }

  /**
   * Maps epoch2 app. attempt IDs to their start times.
   *
   * Takes a line String from th log file which contains the attempt's change to LAUNCHED status.
   * If the line contains an app. attempt ID, it returns a tuple of the form (app. attempt ID, start time).
   * If the line does not contain an app. attempt ID, it returns a tuple of the form ("", "").
   *
   * @param logLine the line from the log file which contains the attempt's change to LAUNCHED status
   * @return tuple (app. attempt ID, start time)
   */
  private def mapAttemptToStartTime(logLine: String):  (String, String) = {

    if (attemptPattern.findFirstIn(logLine).isDefined) {
      (attemptPattern.findFirstIn(logLine).getOrElse(""), logLine.split("INFO")(0).trim)
    }
    else
       ("", "")
  }

  /**
   * Maps app. attempt IDs to their end times and final status.
   *
   * Takes a line String from th log file which contains the final status of the attempt.
   * If the line contains an app. attempt ID, it returns a tuple of the form (app. attempt ID, (end time, final status)).
   * If the line does not contain an app. attempt ID, it returns a tuple of the form ("", ("", "")).
   *
   * @param logLine the line from the log file which contains the final status of the attempt
   * @return tuple (app. attempt ID, (end time, final status))
   */
  private def mapAttemptToEnd(logLine: String):  (String, (String, String)) = {

    if (attemptPattern.findFirstIn(logLine).isDefined) {
      (attemptPattern.findFirstIn(logLine).getOrElse(""),
        (logLine.split("INFO")(0).trim, attemptStatusPattern.findFirstIn(logLine).getOrElse("").split("to")(1).trim))
    }
    else
      ("", ("", ""))
  }

  /**
   * Maps app. attempt IDs to their containers and the host machines of the container.
   *
   * Takes a line String from th log file which contains the a container ID and its host.
   * If the line contains a container ID, it returns a tuple of the form (app. attempt ID, (container nr, host name)).
   * If the line does not contain a container ID, it returns a tuple of the form ("", ("", "")).
   *
   * @param logLine the line from the log file which contains the final status of the attempt
   * @return tuple (app. attempt ID, (container nr, host name))
   */
  private def mapAttemptToContainer(logLine: String):  (String, (String, String))= {

  if (containerPattern.findFirstIn(logLine).isDefined) {
    val container = containerPattern.findFirstIn(logLine).getOrElse("")
    val host = hostPattern.findFirstIn(logLine).getOrElse("").stripSuffix(":").stripPrefix("> on host ")
    ("appattempt_1580812675067_" + container.split("_")(3) + "_0000" + container.split("_")(4), (container.takeRight(1), host))
  }
  else
    ("", ("", ""))
  }

  /**
   * Extracts app. ID from app. attempt ID.
   *
   * Takes an app. attempt ID String and extracts the ID of its associated app.
   * If the attempt ID has the correct timestamp, it returns the corresponding app. ID String.
   *  If the attempt ID does not have the correct timestamp, it returns and empty String "".
   *
   * @param attemptId given  app. attempt ID
   * @return corresponding app. ID
   */
  private def extractAppIdFromAttemptId(attemptId: String): String = {

    if (timestampAppIdPattern.findFirstIn(attemptId).isDefined)
      "application" + timestampAppIdPattern.findFirstIn(attemptId).getOrElse("")
    else
      ""
  }

  /**
   * Checks if an app. ID is from the range of interest for printing.
   *
   * Takes an app. ID String and checks if it is in the range [121, 130].
   * If the app. is in the right range, it returns true. Else, it returns false.
   *
   * @param appId given  app. ID
   * @return Boolean response
   */
  private def isAppToPrint(appId: String): Boolean = {

    if (ep2AppPattern.findFirstIn(appId).isDefined)
      true
    else
      false
  }

  /** Returns the duration of n app expressed in milliseconds. */
  private def extractAppDuration(attemptsList: List[(String, String, String, String, List[(String, String)])]): Long = {
    val timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS")

    timestampFormat.parse(attemptsList.last._3).getTime - timestampFormat.parse(attemptsList.head._2).getTime
  }

  /** Returns the duration of an app. attempt expressed in milliseconds. */
  private def extractAttemptDuration(attempt: (String, String, String, String, List[(String, String)])): Long = {
    val timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS")

    timestampFormat.parse(attempt._3).getTime - timestampFormat.parse(attempt._2).getTime
  }

  /**
   * Writes to file the information associated with a given app. in the requested format.
   *
   * * Takes a tuple containing all the given app's attributes and a provided FileWriter object, then
   * it writes to file * using the provided writer.
   *
   * e.g.
   *
   * ApplicationId : application_1580812675067_0130
   *
   * User     : basil
   *
   * NumAttempts  : 1
   *
   * AttemptNumber : 1
   *
   * StartTime   : 2020-02-13 13:38:41,882
   *
   * EndTime   : 2020-02-13 13:38:52,313
   *
   * FinalStatus   : FINISHING
   *
   * Containers  :(1,iccluster062.iccluster.epfl.ch), (2,iccluster055.iccluster.epfl.ch), (3,iccluster057.iccluster.epfl.ch)
   *
   * @param app tuple containing the given app's attributes
   * @param writer provided FileWriter instance
   * @return Unit
   */
  private def writeAppInfo(app: (String, String, List[(String, String, String, String, List[(String, String)])]), writer: FileWriter): Unit = {

    writer.write("ApplicationId : " + app._1 + "\n\n")
    writer.write("User     : " + app._2 + "\n\n")

    val attempts = app._3
    writer.write("NumAttempts  : " + attempts.length+ "\n\n")

    for (i <- attempts.indices) {
      writer.write("AttemptNumber : " + attempts(i)._1.takeRight(1) + "\n\n")
      writer.write("StartTime   : " + attempts(i)._2 + "\n\n")
      writer.write("EndTime   : " + attempts(i)._3 + "\n\n")
      writer.write("FinalStatus   : " + attempts(i)._4 + "\n\n")
      writer.write("Containers  :" + attempts(i)._5.mkString(", ") + "\n\n")
    }
    writer.write("\n\n")
  }


  def main(args: Array[String]) {

    // Create Spark context.
    val conf = new SparkConf().setMaster("local[*]").setAppName("Milestone1")
    val sc = SparkContext.getOrCreate(conf)

    val appsLines = sc.textFile(args(0)).filter(_.contains("_1580812675067_"))

    val appsToLines = appsLines.map(mapAppToLines)

    val appsToUsers = appsToLines.filter(_._2.contains("user")).map(mapAppToUser).filter(_._2 != "").distinct

    val attemptsLines = appsLines.filter(_.contains("appattempt_1580812675067_"))

    val attemptsStartTimes = attemptsLines.filter(_.contains(" to LAUNCHED")).map(mapAttemptToStartTime).filter(_._2 != "").distinct

    val attemptsEndTimes = attemptsLines.filter(_.contains("from FINAL_SAVING to")).map(mapAttemptToEnd).filter(_._1 != "").distinct

    val attemptsContainers = {
      appsLines.filter(_.contains("container_")).filter(_.contains("> on host"))
        .map(mapAttemptToContainer).filter(_._1 != "").groupByKey.distinct
    }

    val attemptsInfo = {
      attemptsStartTimes.join(attemptsEndTimes).join(attemptsContainers)
        .map(el => (extractAppIdFromAttemptId(el._1), (el._1, el._2._1._1, el._2._1._2._1, el._2._1._2._2, el._2._2.toList))).groupByKey
    }

    val appsInfo = appsToUsers.join(attemptsInfo).map(el => (el._1, el._2._1, el._2._2.toList.sortBy(_._1))).sortBy(_._1)

    val appsToPrint = appsInfo.filter(app => isAppToPrint(app._1))

    // Initialize FileWriter object that will write to the`answers.txt` file.
    val writer = new FileWriter("answers.txt", true)

    appsToPrint.collect.toList.foreach(writeAppInfo(_, writer))

    val userWithMaxApps = appsToUsers.groupBy(_._2).mapValues(_.size).collect.maxBy(_._2)
    writer.write("1. " + userWithMaxApps._1 +", " + userWithMaxApps._2 + "\n\n")

    val userWithMaxUnsuccessful = {
      appsInfo.map(el => (el._2, el._3)).groupByKey.map(el => (el._1, el._2.flatten))
        .map(el => (el._1, el._2.count(_._4 != "FINISHING"))).collect.maxBy(_._2)
    }
    writer.write("2. " + userWithMaxUnsuccessful._1 +", " + userWithMaxUnsuccessful._2 + "\n\n")

    val appsStartedPerDates = {
      appsInfo.map(el => (el._3.head._2, el._3.head._1)).map(el => (el._1.split(" ").head, el._2)).groupByKey
        .map(el => (el._1, el._2.size)).collect.toList
    }
    writer.write("3. ")
    for (i <- 0 until (appsStartedPerDates.length-1)) {
      writer.write(appsStartedPerDates(i)._1 + ": " + appsStartedPerDates(i)._2 + ", ")
    }
    writer.write(appsStartedPerDates.last._1 + ": " + appsStartedPerDates.last._2 + "\n\n")

    val meanAppDuration = appsInfo.map(el => (el._1, el._3)).map(el => extractAppDuration(el._2)).mean.toInt
    writer.write("4. %d\n\n".format(meanAppDuration))

    val meanSuccessfulAttemptDuration =  attemptsInfo.flatMap(_._2.toList).filter(_._4 == "FINISHING").map(extractAttemptDuration).mean.toInt
    writer.write("5. %d\n\n".format(meanSuccessfulAttemptDuration))

    val machines = attemptsInfo.flatMap(_._2.toList).flatMap(_._5).map(_._2).distinct.collect.sorted
    writer.write("6. %d, %s\n\n".format(machines.length, machines.toList.mkString(", ")))

    val appToMachines = {
      appsInfo.map(el => (el._1, el._3.flatMap(_._5).map(_._2))).flatMap(el => el._2.map((_,  el._1))).groupByKey
        .map(el => (el._1, el._2.toSet.size)).collect.maxBy(_._2)
    }
    writer.write("7. %s, %d\n".format(appToMachines._1, appToMachines._2))

    // Close FileWriter.
    writer.close()
  }
}

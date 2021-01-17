package com.mathebotond.wla

import com.mathebotond.wla.io.Input
import com.mathebotond.wla.model.{Session, SingleAccess, UserSessions}
import org.apache.spark.sql.{Dataset, SparkSession}

object LogAnalytics extends SparkJob {

  override def appName: String = "Log Analytics"

  override def run(spark: SparkSession, config: UsageConfig, input: Input): Unit = {
    val logs = input.readAndParse(config.input);

    val sessions = transformLogsToSessions(spark, logs);

    sessions.show(5, truncate = false)
  }

  implicit object SessionOrdering extends Ordering[Session] {
    def compare(o1: Session, o2: Session): Int = o1.start compareTo o2.start
  }

  def transformLogsToSessions(spark: SparkSession, logs: Dataset[SingleAccess]): Dataset[Session] = {
    import spark.implicits._

    logs.map(log => {
      val session = Session(log.ip, log.timestamp.getTime, log.timestamp.getTime, 0, Map(log.endpoint -> 1))
      (log.ip, UserSessions(log.ip, List(session)))
    })
      .groupByKey(item => item._1) // group 1 request sessions by IP address
      .reduceGroups(reduceSessions)
      .flatMap(session => session._2._2.sessionByStart)
  }

  def reduceSessions: ((String, UserSessions), (String, UserSessions)) => (String, UserSessions) =
    (keyValue1, keyValue2) => {
      val userSessions1 = keyValue1._2
      val userSessions2 = keyValue2._2

      val merged = ListUtil.mergeOrdered(userSessions1.sessionByStart, userSessions2.sessionByStart, SessionOrdering)

      var lastReduced = merged.head
      var reduced = List[Session](lastReduced)

      for (index <- 1 until merged.size) {
        if (lastReduced.end + 15000 > merged(index).start) {
          lastReduced.reduceWith(merged(index))
        } else {
          reduced ::= merged(index)
          lastReduced = merged(index)
        }
      }

      (keyValue1._1, UserSessions(userSessions1.ip, reduced))
    }
}

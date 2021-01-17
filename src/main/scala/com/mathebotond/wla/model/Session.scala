package com.mathebotond.wla.model

import scala.language.postfixOps

case class Session(ip: String, var start: Long, var end: Long, var length: Long, var urlVisits: Map[String, Long]) {
  def reduceWith(mergeWith: Session): Unit = {
    start = Math.min(start, mergeWith.start)
    end = Math.max(end, mergeWith.end)
    length = end - start

    urlVisits = (urlVisits.keySet ++ mergeWith.urlVisits.keySet) map { url => url -> {
      if (!urlVisits.contains(url)) {
        mergeWith.urlVisits(url)
      } else if (! mergeWith.urlVisits.contains(url)) {
        urlVisits(url)
      } else {
        mergeWith.urlVisits(url) + urlVisits(url)
      }}
    } toMap
  }
}

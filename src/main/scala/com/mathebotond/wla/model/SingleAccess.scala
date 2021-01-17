package com.mathebotond.wla.model

import java.sql.Date

case class SingleAccess(timestamp: Date, ip: String, endpoint: String)

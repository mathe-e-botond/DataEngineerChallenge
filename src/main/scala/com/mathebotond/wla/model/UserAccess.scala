package com.mathebotond.wla.model

import java.sql.Date

case class UserAccess (timestamp: Date, ip: String, endpoint: String)

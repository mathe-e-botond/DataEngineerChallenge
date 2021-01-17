package com.mathebotond.wla

import scopt.OptionParser

case class UsageConfig(input: String = "")

class UsageOptionParser
  extends OptionParser[UsageConfig]("job config") {
  head("scopt", "4.x")

  help("help").text("Prints this usage text")

  arg[String]("<file>...")
    .unbounded()
    .required()
    .action((value, arg) => arg.copy(input = value))
    .text("optional unbounded args")
}

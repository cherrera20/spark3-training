val Spark = "3.0.1"
val Delta = "0.8.0"

/** ********* PROJECT INFO ******************/
name := "training spark"
version := "1.0"

/** ********* PROJECT SETTINGS ******************/
Configuration.settings

/** ********* PROD DEPENDENCIES *****************/
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % Spark,
  "org.apache.spark" %% "spark-sql" % Spark,
  "org.apache.spark" %% "spark-hive" % Spark

)

/** ********* TEST DEPENDENCIES *****************/
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.2" % Test,
  "io.delta" %% "delta-core" % Delta % Test,
)

/** ********* COMMANDS ALIASES ******************/
addCommandAlias("t", "test")
addCommandAlias("to", "testOnly")
addCommandAlias("tq", "testQuick")
addCommandAlias("tsf", "testShowFailed")

addCommandAlias("c", "compile")
addCommandAlias("tc", "test:compile")

addCommandAlias("f", "scalafmt")             // Format production files according to ScalaFmt
addCommandAlias("fc", "scalafmtCheck")       // Check if production files are formatted according to ScalaFmt
addCommandAlias("tf", "test:scalafmt")       // Format test files according to ScalaFmt
addCommandAlias("tfc", "test:scalafmtCheck") // Check if test files are formatted according to ScalaFmt

// All the needed tasks before pushing to the repository (compile, compile test, format check in prod and test)
addCommandAlias("prep", ";c;tc;fc;tfc")

import sbt._

object Dependencies {
  val framelessVersion = "0.8.0" // for Spark 2.4.0
  lazy val zio = "dev.zio" %% "zio" % "1.0.0-RC11-1"
  lazy val zioInteropCats = "dev.zio" %% "zio-interop-cats" % "2.0.0.0-RC2"
  lazy val framelessDataset = "org.typelevel" %% "frameless-dataset" % framelessVersion
  lazy val framelessCats = "org.typelevel" %% "frameless-cats" % framelessVersion
  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % "2.4.3"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
}

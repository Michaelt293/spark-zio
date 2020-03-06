import sbt._

object Dependencies {
  lazy val zio = "dev.zio" %% "zio" % "1.0.0-RC18-1"
  lazy val sparkSql = "org.apache.spark" %% "spark-sql" % "2.4.3"
  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.0.5"
}

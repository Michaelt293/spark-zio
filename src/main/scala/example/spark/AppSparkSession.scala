package example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import zio._

object AppSparkSession {
  type AppSparkSession = Has[Service]

  trait Service {
    val sparkSession: Task[SparkSession]
  }

  val sparkSession: RIO[AppSparkSession, SparkSession] =
    RIO.accessM(_.get.sparkSession)

  val live: ZLayer.NoDeps[Nothing, AppSparkSession] = ZLayer.succeed(
    new Service {
      val sparkSession: Task[SparkSession] =
        ZIO.effect {
          val conf =
            new SparkConf()
              .set("spark.ui.enabled", "false")
              .set("spark.driver.host", "localhost")

          SparkSession.builder
            .config(conf)
            .master("local")
            .appName("ZioAppLive")
            .getOrCreate()
        }
    }
  )

  val test: ZLayer.NoDeps[Nothing, AppSparkSession] = ZLayer.succeed(
    new Service {
      val sparkSession: Task[SparkSession] =
        ZIO.effect {
          val conf =
            new SparkConf()
              .set("spark.ui.enabled", "false")
              .set("spark.driver.host", "localhost")

          SparkSession.builder
            .config(conf)
            .master("local")
            .appName("ZioAppTest")
            .getOrCreate()
        }
    }
  )
}

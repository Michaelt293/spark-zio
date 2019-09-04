package example

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import zio._

trait AppSparkSession extends Serializable {
  val appSparkSession: AppSparkSession.Service[Any]
}

object AppSparkSession extends Serializable {
  trait Service[R] {
    val sparkSession: RIO[R, SparkSession]
  }

  val sparkSession: RIO[AppSparkSession, SparkSession] =
    RIO.accessM(_.appSparkSession.sparkSession)

  trait Live extends AppSparkSession {
    val appSparkSession: AppSparkSession.Service[Any] =
      new AppSparkSession.Service[Any] {
        val sparkSession: Task[SparkSession] =
          ZIO.effect(
            SparkSession.builder
              .master("local")
              .appName("ZioAppLive")
              .getOrCreate()
          )
      }
  }

  object Live extends Live

  trait Test extends AppSparkSession {
    val appSparkSession: AppSparkSession.Service[Any] =
      new AppSparkSession.Service[Any] {
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
  }

  object Test extends Test
}

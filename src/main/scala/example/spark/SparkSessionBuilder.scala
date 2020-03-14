package example.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import zio.{Layer, ZLayer}

object SparkSessionBuilder {

  trait Service {
    val sparkSessionBuilder: SparkSession.Builder
  }

  val live: Layer[Nothing, SparkSessionBuilder] = ZLayer.succeed(
    new Service {
      val sparkSessionBuilder: SparkSession.Builder = {
        val conf =
          new SparkConf()
            .set("spark.ui.enabled", "false")
            .set("spark.driver.host", "localhost")

        SparkSession.builder
          .config(conf)
          .master("local")
          .appName("ZioAppLive")
      }
    }
  )
}

package example

import org.apache.spark.sql.SparkSession
import zio.{Has, RIO, ZIO, ZLayer, ZManaged}

package object spark {

  type SparkSessionBuilder = Has[SparkSessionBuilder.Service]
  type AppSparkSession = Has[SparkSession]

  val sparkSessionZLayer
    : ZLayer[SparkSessionBuilder, Throwable, AppSparkSession] =
    ZLayer.fromManaged {
      ZManaged.make {
        ZIO.access[SparkSessionBuilder](_.get).flatMap {
          service: SparkSessionBuilder.Service =>
            ZIO.effect(service.sparkSessionBuilder.getOrCreate())
        }
      }(sparkSession => ZIO.succeed(sparkSession.stop()))
    }

  val sparkSession: RIO[AppSparkSession, SparkSession] =
    ZIO.access[AppSparkSession](_.get)
}

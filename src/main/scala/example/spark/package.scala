package example

import org.apache.spark.sql.SparkSession
import zio._

package object spark {

  type SparkSessionBuilder = Has[SparkSessionBuilder.Service]
  type AppSparkSession = Has[SparkSession]

  val sparkSessionZLayer
    : ZLayer[SparkSessionBuilder, Throwable, AppSparkSession] =
    ZLayer.fromManaged {
      ZManaged.make {
        ZIO.access[SparkSessionBuilder](_.get).flatMap { service =>
          Task.effect(service.sparkSessionBuilder.getOrCreate())
        }
      }(sparkSession => Task.succeed(sparkSession.stop()))
    }

  val sparkSession: RIO[AppSparkSession, SparkSession] =
    ZIO.access[AppSparkSession](_.get)
}

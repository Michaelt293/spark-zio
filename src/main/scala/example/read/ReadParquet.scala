package example.read

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import zio._

object ReadParquet {

  trait Service {
    def readParquet[A <: Product](spark: SparkSession, path: String)(
        implicit
        typeTag: TypeTag[A]
    ): Task[Dataset[A]]
  }

  val live: ZLayer.NoDeps[Nothing, ReadParquet] = ZLayer.succeed(
    new Service {
      def readParquet[A <: Product](spark: SparkSession, path: String)(
          implicit
          typeTag: TypeTag[A]
      ): RIO[Any, Dataset[A]] =
        ZIO.effect(
          spark.read
            .parquet(path)
            .as[A](Encoders.product[A])
        )
    }
  )
}

package example.read

import scala.reflect.runtime.universe.TypeTag
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import zio.{RIO, Task, ZLayer}

object ReadCsv {

  trait Service {
    def readCsv[A <: Product](spark: SparkSession, path: String)(
        implicit
        typeTag: TypeTag[A]
    ): Task[Dataset[A]]
  }

  val live: ZLayer.NoDeps[Nothing, ReadCsv] = ZLayer.succeed(
    new Service {
      def readCsv[A <: Product](spark: SparkSession, path: String)(
          implicit
          typeTag: TypeTag[A]
      ): RIO[Any, Dataset[A]] =
        RIO.effect(
          spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(path)
            .as[A](Encoders.product[A])
        )
    }
  )
}

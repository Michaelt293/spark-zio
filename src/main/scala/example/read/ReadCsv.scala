package example.read

import scala.reflect.ClassTag

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import zio._

object ReadCsv {
  type ReadCsv = Has[Service]

  trait Service {
    def readCsv[A](spark: SparkSession, path: String)(
        implicit
        classTag: ClassTag[A]
    ): Task[Dataset[A]]
  }

  def readCsv[A](spark: SparkSession, path: String)( // TODO: move
      implicit
      classTag: ClassTag[A]
  ): RIO[ReadCsv, Dataset[A]] =
    RIO.accessM(_.get.readCsv(spark, path))

  val live: ZLayer.NoDeps[Nothing, ReadCsv] = ZLayer.succeed(
    new Service {
      def readCsv[A](spark: SparkSession, path: String)(
          implicit
          classTag: ClassTag[A]
      ): RIO[Any, Dataset[A]] =
        ZIO.effect(
          spark.read.csv(path).as[A](Encoders.kryo[A])
        )
    }
  )
}

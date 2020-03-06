package example.read

import scala.reflect.ClassTag

import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import zio._

trait ReadCsv extends Serializable {
  def readCsv: ReadCsv.Service[Any]
}

object ReadCsv {
  trait Service[R] {
    def readCsv[A](spark: SparkSession, path: String)(
        implicit
        classTag: ClassTag[A]
    ): RIO[R, Dataset[A]]
  }

  def readCsv[A](spark: SparkSession, path: String)( // TODO: move
      implicit
      classTag: ClassTag[A]
  ): RIO[ReadCsv, Dataset[A]] =
    RIO.accessM(_.readCsv.readCsv(spark, path))

  trait Live extends ReadCsv {
    override def readCsv: ReadCsv.Service[Any] =
      new ReadCsv.Service[Any] {
        override def readCsv[A](spark: SparkSession, path: String)(
            implicit
            classTag: ClassTag[A]
        ): RIO[Any, Dataset[A]] =
          ZIO.effect(
            spark.read.csv(path).as[A](Encoders.kryo[A])
          )
      }
  }

  object Live extends Live
}

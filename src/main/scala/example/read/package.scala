package example

import org.apache.spark.sql.{Dataset, SparkSession}
import zio.{Has, RIO}

import scala.reflect.ClassTag

package object read {

  type ReadCsv = Has[ReadCsv.Service]
  type ReadParquet = Has[ReadParquet.Service]

  def readCsv[A](spark: SparkSession, path: String)(
      implicit
      classTag: ClassTag[A]
  ): RIO[ReadCsv, Dataset[A]] =
    RIO.accessM(_.get.readCsv(spark, path))

  def readParquet[A](spark: SparkSession, path: String)(
      implicit
      classTag: ClassTag[A]
  ): RIO[ReadParquet, Dataset[A]] =
    RIO.accessM(_.get.readParquet(spark, path))
}

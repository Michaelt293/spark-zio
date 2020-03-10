package example

import org.apache.spark.sql.{Dataset, SparkSession}
import zio.{Has, RIO}

package object write {

  type WriteCsv = Has[WriteCsv.Service]
  type WriteParquet = Has[WriteParquet.Service]

  def writeCsv[A](
      spark: SparkSession,
      path: String,
      dataset: Dataset[A]
  ): RIO[WriteCsv, Unit] =
    RIO.accessM(_.get.writeCsv(spark, path, dataset))

  def writeParquet[A](
      spark: SparkSession,
      path: String,
      dataset: Dataset[A]
  ): RIO[WriteParquet, Unit] =
    RIO.accessM(_.get.writeParquet(spark, path, dataset))
}

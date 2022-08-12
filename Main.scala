package com.modak

import com.github.music.of.the.ainur.almaren.Almaren
import com.github.music.of.the.ainur.almaren.builder.Core.Implicit
import org.apache.spark.sql.{AnalysisException, Column, DataFrame, SaveMode, SparkSession}
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success, Try}
import io.circe.parser.{decode, parse}
import io.circe.generic.auto._

import scala.Console.println

object Main extends LazyLogging {

  val almaren = Almaren("Sample")

  val spark: SparkSession = Almaren.spark.enableHiveSupport().getOrCreate()

  def main(args: Array[String]): Unit = {
    Try {
      almaren.builder.sourceJdbc("jdbc:postgresql://w3.training5.modak.com:5432/training","org.postgresql.Driver","select * from Sample890",Some("mt4088"),Some("mt4088@m02y22"),Map()).targetJdbc("jdbc:postgresql://w3.training5.modak.com:5432/training","org.postgresql.Driver","target890",SaveMode.Overwrite,Some("mt4088"),Some("mt4088@m02y22"),Map()).batch
    } match {
      case Success(j) =>
        logger.info("Json parsing completed successfully")
      case Failure(f) =>
        logger.error(s"Json Parsing failed with the exception : ${f.getLocalizedMessage}")
        throw f
    }
  }

    def decodeBase64String(str: String): String = {
      new String(java.util.Base64.getDecoder.decode(str))
    }

    def jsonParser(str: String): JSON = {
      logger.info("Started Json parsing")
      decode[JSON](str) match {
        case Right(json) => json
        case Left(exception) =>
          logger.error(s"Json Parsing Failed with Exception ${exception.getLocalizedMessage}")
          logger.error("Invalid Input JSON Provided")
          throw exception
      }
    }
}

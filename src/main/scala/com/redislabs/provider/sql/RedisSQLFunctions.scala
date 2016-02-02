package com.redislabs.provider.sql

import org.apache.spark.sql.SQLContext
import breeze.linalg.Vector

import scala.collection.mutable.HashMap

/**
 * RedisSQLContext is used to import and make use of mapSeries function for instant and
 * observation dataframe. It works by implicitly casting an SQLContext to RedisSQLContext.
 */
class RedisSQLContext(val sc: SQLContext) extends Serializable {
  def setMapSeries(funcName: String, mapSeries: (Vector[Double] => Vector[Double])) = {
    RedisSQLContext.funcMap += (funcName -> mapSeries)
  }
  def getMapSeries(funcName: String): (Vector[Double] => Vector[Double]) = {
    RedisSQLContext.funcMap.getOrElse(funcName, (x: Vector[Double]) => x)
  }
}

object RedisSQLContext {
  private val funcMap = new HashMap[String, (Vector[Double] => Vector[Double])]()
}

trait RedisSQLFunctions {
  implicit def toRedisSQLContext(sc: SQLContext): RedisSQLContext = new RedisSQLContext(sc)
}

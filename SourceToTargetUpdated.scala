/*
 * IBM Confidential
 *
 * Licensed Materials - Property of IBM
 * (C) Copyright IBM Corp. 2019  All Rights Reserved
 * US Government Users Restricted Rights - Use, duplication or
 * disclosure restricted by GSA ADP Schedule Contract with IBM Corp.
 */
package com.ibm.cedp.garage.common.ingestion

import com.ibm.cedp.garage.common.ingestion.repositories.{CosConnection, JdbcConnection}
import com.ibm.cedp.garage.common.ingestion.services._
import com.ibm.cedp.garage.common.ingestion.util.Util
import org.apache.log4j.{Level, LogManager, Logger}

object SourceToTarget {
  private val logger = Logger.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    LogManager.getRootLogger.setLevel(if (args.length > 0) Level.toLevel(args(0)) else Level.INFO)

    val sourceConf = Util.getConfig("spark.source.")
    val targetConf = Util.getConfig("spark.target.")

    val start = System.currentTimeMillis
    logger.info(s"Start loading source data...")

    val sourceService = sourceConf.get("type").map(t => t.toLowerCase) match {
      case Some("cos") | Some("fs") => new CosSourceService(new CosConnection(sourceConf), sourceConf)
      case Some("db")               => new DbSourceService(new JdbcConnection(sourceConf), sourceConf) 
      case _                        => throw new IllegalArgumentException("Indicated source type is invalid.")
    }

    val (df, objectType) = sourceService.read()
    //logger.info("Number of partitions: " + df.rdd.getNumPartitions)
    df.printSchema()
	System.out.println("Showing first 10 rows")
	df.show(10)

    if (df.isEmpty) {
      logger.warn("Loaded source data is empty")
    } else {
      val targetService = targetConf.get("type").map(t => t.toLowerCase) match {
        case Some("cos") | Some("fs") => new CosTargetService(new CosConnection(targetConf), targetConf)
        case Some("db")               => new DbTargetService(new JdbcConnection(targetConf), targetConf)
        case _                        => throw new IllegalArgumentException("Indicated target type is invalid.")
      }

      val isFullRefresh = targetConf.getOrElse("isFullRefresh", "true").toBoolean
      targetService.write(df, sourceService.getCountRange, isFullRefresh, Some(objectType))

      val duration = Util.getDuration(start)

      logger.info(s"The process finished and took $duration")
    }
  }
}

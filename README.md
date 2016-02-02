[![Build Status](https://travis-ci.org/RedisLabs/spark-timeseries.svg)](https://travis-ci.org/RedisLabs/spark-timeseries)

spark-timeseries
=============

A Scala / Python library for interacting with time series data on Apache Spark.

This fork extends [Cloudera's spark-timeseries](https://github.com/cloudera/spark-timeseries) library with:

* `RedisTimeSeriesRDD` that uses Redis' Sorted Sets as data source
* Utility for generating sample dataset files - `com.redislabs.provider.util.GenerateWorkdayTestData`
* Import tool that loads time series files to Redis - `com.redislabs.provider.util.ImportTimeSeriesData`
* A test suite that compares execution times of common queries using different caching strategies can be found at `src/main/scala/com/run/Main.scala`

**Note:** The redis extension is only implemented in the Scala library at the moment.

Time series storage in Redis
---

Each time series in the dataset is stored as a Redis Sorted Set. The Sorted Set's key name is the name of the time series. The set's members contain the sampled values and  scores are set to the respective timestamp. Filters on keys, column names and date ranges are pushed down to Redis via the `RedisTimeSeriesRDD` for processing the data close to where it is stored and reduce traffic between Spark's workers and the storage. The `RedisTimeSeriesRDD` can be transformed to a `TimeSeriesRDD` to provide its full functionality.

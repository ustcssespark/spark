/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.examples.sql

// $example on:programmatic_schema$
import org.apache.spark.sql.Row
// $example off:programmatic_schema$
// $example on:init_session$
import org.apache.spark.sql.SparkSession
// $example off:init_session$
// $example on:programmatic_schema$
// $example on:data_types$
import org.apache.spark.sql.types._
// $example off:data_types$
// $example off:programmatic_schema$

object SparkSaveModeExample {

  // $example on:create_ds$
  case class Person(name: String, age: Long)
  // $example off:create_ds$

  def main(args: Array[String]): Unit = {
    // $example on:init_session$
    val spark = SparkSession
      .builder()
      .appName("Spark SaveMode basic example")
      .config("spark.some.config.option", "some-value")
      .config("spark.driver.extraClassPath", "../spark_env/postgresql-42.2.9.jar")
      .config("spark.executor.extraClassPath", "../spark_env/postgresql-42.2.9.jar")
      .getOrCreate()
    // For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._
    // $example off:init_session$
    runSparkSaveModeDeleteExample(spark)
    runSparkSaveModeUpdateExample(spark)
    clean(spark)
    runSparkSaveModeUpsertExample(spark)
    spark.stop()
  }

  private def clean(spark: SparkSession): Unit = {
    val df = spark.read.json("examples/src/main/resources/people.json")
    df.write
      .format("jdbc")
      .mode("delete")
      .option("url", "jdbc:postgresql://localhost:5433/spark")
      .option("dbtable", "people")
      .option("user", "spark")
      .option("password", "123456")
      .save()
    val df2 = spark.read.json("examples/src/main/resources/people2.json")
    df2.write
      .format("jdbc")
      .mode("delete")
      .option("url", "jdbc:postgresql://localhost:5433/spark")
      .option("dbtable", "people")
      .option("user", "spark")
      .option("password", "123456")
      .save()
  }

  private def runSparkSaveModeUpsertExample(spark: SparkSession): Unit = {
    println("==========================Upsert Example===========================")
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5433/spark")
      .option("dbtable", "people")
      .option("user", "spark")
      .option("password", "123456")
      .load()
    val df = spark.read.json("examples/src/main/resources/people.json")
    println("datafram via jdbc")
    jdbcDF.show()
    println("dataframe via json")
    df.show()
    df.write
      .format("jdbc")
      .mode("append")
      .option("url", "jdbc:postgresql://localhost:5433/spark")
      .option("dbtable", "people")
      .option("user", "spark")
      .option("password", "123456")
      .save()
    println("datafram via jdbc after insert")
    jdbcDF.show()
    val df2 = spark.read.json("examples/src/main/resources/people2.json")
    println("dataframe via json to update")
    df2.show()
    df2.write
      .format("jdbc")
      .mode("upsert")
      .option("url", "jdbc:postgresql://localhost:5433/spark")
      .option("dbtable", "people")
      .option("user", "spark")
      .option("password", "123456")
      .save()
    println("datafram via jdbc after upsert")
    jdbcDF.show()
  }

  private def runSparkSaveModeUpdateExample(spark: SparkSession): Unit = {
    println("==========================Update Example===========================")
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5433/spark")
      .option("dbtable", "people")
      .option("user", "spark")
      .option("password", "123456")
      .load()
    val df = spark.read.json("examples/src/main/resources/people.json")
    println("datafram via jdbc")
    jdbcDF.show()
    println("dataframe via json")
    df.show()
    df.write
      .format("jdbc")
      .mode("append")
      .option("url", "jdbc:postgresql://localhost:5433/spark")
      .option("dbtable", "people")
      .option("user", "spark")
      .option("password", "123456")
      .save()
    println("datafram via jdbc after insert")
    jdbcDF.show()
    val df2 = spark.read.json("examples/src/main/resources/people2.json")
    println("dataframe via json to update")
    df2.show()
    df2.write
      .format("jdbc")
      .mode("update")
      .option("url", "jdbc:postgresql://localhost:5433/spark")
      .option("dbtable", "people")
      .option("user", "spark")
      .option("password", "123456")
      .save()
    println("datafram via jdbc after update")
    jdbcDF.show()
  }

  private def runSparkSaveModeDeleteExample(spark: SparkSession): Unit = {
    println("==========================Delete Example===========================")
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://localhost:5433/spark")
      .option("dbtable", "people")
      .option("user", "spark")
      .option("password", "123456")
      .load()
    val df = spark.read.json("examples/src/main/resources/people.json")
    println("datafram via jdbc")
    jdbcDF.show()
    println("dataframe via json")
    df.show()
    df.write
      .format("jdbc")
      .mode("append")
      .option("url", "jdbc:postgresql://localhost:5433/spark")
      .option("dbtable", "people")
      .option("user", "spark")
      .option("password", "123456")
      .save()
    println("datafram via jdbc")
    jdbcDF.show()
    df.write
      .format("jdbc")
      .mode("delete")
      .option("url", "jdbc:postgresql://localhost:5433/spark")
      .option("dbtable", "people")
      .option("user", "spark")
      .option("password", "123456")
      .save()
    println("datafram via jdbc afer delete")
    jdbcDF.show()
  }

}

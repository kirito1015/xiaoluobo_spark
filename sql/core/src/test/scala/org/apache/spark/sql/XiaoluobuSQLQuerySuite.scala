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

package org.apache.spark.sql

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.test.{SharedSparkSession}

class XiaoluobuSQLQuerySuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {


  setupTestData()

  test("SPARK-19059: read file based table whose name starts with underscore") {
    withTable("_tbl") {
      sql("CREATE TABLE `_tbl`(i INT) USING parquet")
      val analyzedDF = sql("INSERT INTO `_tbl` VALUES (1), (2), (3)")
      val ana = analyzedDF.queryExecution.analyzed
      // scalastyle:off println
      println("== Analyzed Logical Plan ==" )
      println( ana)
      println("== Optimized Logical Plan ==" )
      println(analyzedDF.queryExecution.optimizedPlan)
      println("== Physical Plan ==" )
      println(analyzedDF.queryExecution.sparkPlan)
      println("== executedPlan ==" )
      println(analyzedDF.queryExecution.executedPlan)
    }
  }


  test("logical test") {
    // val df = Seq((1, 1)).toDF("a", "b")
    // df.createOrReplaceTempView("testData2")

    // scalastyle:off println
    // println("--------***********--------" )
    // println(analyzedDF)
    // println(analyzedDF.queryExecution.analyzed)
    // println(analyzedDF.queryExecution.optimizedPlan)
    // println(analyzedDF.queryExecution.executedPlan)
    // scalastyle:on println

    // val analyzedDF = sql("SELECT A,B, count(1) FROM TESTDATA2 WHERE A>2 group by a,b")
    // val analyzedDF = sql("SELECT A,B, COUNT(1) AS CNT FROM TESTDATA2 WHERE A>2 GROUP BY A,B ")
    // val analyzedDF = sql("SELECT A,B, COUNT(1) AS CNT,row_number() over(partition by A order
    // by B desc) as rn" +
    // " FROM TESTDATA2 WHERE A>2 GROUP BY A,B ")
    // val analyzedDF = sql("create table aaa as SELECT A,B, COUNT(1) ,
    // row_number() over(partition by A order by B desc) " +
    // " FROM TESTDATA2 WHERE A>2 GROUP BY A,B ")
    // scalastyle:off println
    // val analyzedDF = sql("WITH ATABLE AS (SELECT * FROM TESTDATA2) SELECT A,B, count(1)
    // FROM ATABLE WHERE A>2 group by a,b")
    // val analyzedDF = sql("SELECT A,B, COUNT(1)  FROM TESTDATA2 WHERE A>2 GROUP BY A,B")
    // val analyzedDF = sql("SELECT T1.COL1, T2.COL2 FROM TESTDATA T1,T2 ")
    // val analyzedDF = sql("SELECT T1.COL1, T2.COL2 FROM TESTDATA T1 JOIN T2  ON T1.ID=T2.ID ")
    // val analyzedDF = sql("SELECT T1.COL1, T2.COL2 FROM TESTDATA T1 JOIN T2
    // ON T1.ID=T2.ID AND `(a)?+.+` = 2 ")
    // val analyzedDF = sql("SELECT * FROM (SELECT YEAR, COURSE, EARNINGS FROM COURSESALSES)
    // PIVOT ( SUM(EARNINGS) FOR COURSE IN ('dotNET' AS FIRST, 'Java' AS SEC)) ")
    // val analyzedDF = sql("SELECT T1.COL1,T1.COL2 FROM TESTDATA T1 LATERAL VIEW EXPLODE(T1.ID)
    // ID_TABLE1 AS TID LATERAL VIEW EXPLODE(T1.COL) ID_TABLE2 AS MID ")
    // val analyzedDF = sql("SELECT A ,B, COUNT(1) AS CNT FROM TESTDATA2 WHERE A>2 GROUP BY A,B")
    // val analyzedDF = sql("select a,xmc from (select
    // A,B as xmc from testdata2 where a>2 )
    // tmp where xmc>3 ")
    // val analyzedDF = sql("select b+1 as b1,b,sum(a)+1 as sc ,sum(A+b) as ab,sum(B+a) as ba,count(distinct a),count(distinct a+b) from testdata2 where b>3 group by b+1,b")
    val analyzedDF = sql("select b,count(a) as a1,sum(a+b) as a2,sum(b+a) as a3,sum(a)+1 as a4  from testdata2 where b<2 group by b,b+1")
    // val analyzedDF = sql("select b,sum(a+b)/count(distinct a) as a2,sum(b+a)/count(distinct a) as a3  from testdata2 where b<2 group by b")
    // val analyzedDF = sql("select a,b,id from (select  A,B,monotonically_increasing_id()
    // as id from testdata2  where a>2 )tmp  where  b<1 ")
    // val analyzedDF = sql("SELECT CONCAT('A','B') ")
    println()
    // analyzedDF.queryExecution.debug.codegen()

    // println("--------***********--------" )
    // println("11111" + analyzedDF.queryExecution.toString)
    // println(analyzedDF.queryExecution.logical.prettyJson)
    val ana = analyzedDF.queryExecution.analyzed
    println("== Analyzed Logical Plan ==" )
    println( ana)
    println("== Optimized Logical Plan ==" )
    val opt=analyzedDF.queryExecution.optimizedPlan
    println(analyzedDF.queryExecution.optimizedPlan)
    println(opt)
    println("== Physical Plan ==" )
    println(analyzedDF.queryExecution.sparkPlan)
    println("== executedPlan ==" )
    println(analyzedDF.queryExecution.executedPlan)

    // println(analyzedDF.queryExecution.executedPlan.productElement(0))

    //  mapProductIterator(analyzedDF.queryExecution.executedPlan.productElement(0)
    //    .asInstanceOf[SparkPlan])
    // scalastyle:on println

    def mapProductIterator ( plan : SparkPlan ): Array[Any] = {
      val arr = Array.ofDim[Any](plan.productArity)
      var i = 0
      while (i < arr.length) {
        arr(i) = plan.productElement(i)
        // scalastyle:off println
        println("######" + arr(i))
        // scalastyle:on println
        i += 1
      }
      arr
    }

  }
}

    
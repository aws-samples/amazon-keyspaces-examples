/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */
import com.datastax.oss.driver.api.core.CqlSession

import scala.jdk.CollectionConverters._

object SampleConnectionWithSigv4 {

  // This code uses the configuration present in ./resources/application.conf
  // and the AWS default credential chain.
  // see https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html.
  def main(args: Array[String]): Unit = {
    val resultSet = session.execute("select * from system_schema.keyspaces")
    val rows = resultSet.all().asScala
    rows.foreach(println)

    println("List of all Keyspaces in this region...")
    for (row <- rows) println(row.getString("keyspace_name"))
  }

  private val session = CqlSession.builder.build()
}

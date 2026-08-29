/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spark.bigtable.datasources.config.client

import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings
import com.google.cloud.bigtable.data.v2.BigtableDataSettings
import com.google.cloud.spark.bigtable.Logging
import com.google.cloud.spark.bigtable.datasources.BigtableSparkConfBuilder
import io.grpc.internal.GrpcUtil.USER_AGENT_KEY
import org.scalatest.funsuite.AnyFunSuite

class UserAgentConfigTest extends AnyFunSuite with Logging {

  // Verify that the default UserAgentConfig constructor builds a valid, non-empty user-agent string without nulls.
  test("UserAgentConfig handles default values without null errors") {
    val config = UserAgentConfig()
    val text = config.userAgentText

    assert(text != null && text.nonEmpty)
    assert(!text.contains("null"))
    assert(!text.contains("  "))
    assert(text.startsWith("spark-bigtable/"))
  }

  // Verify null-safety: passing null or empty fields must not throw NPEs or format "null" into the string.
  test("UserAgentConfig handles null and empty fields gracefully") {
    val configWithNulls = UserAgentConfig(
      connectorArtifactId = "spark-bigtable",
      connectorVersion = "0.10.1",
      sparkVersion = "3.5.1",
      scalaVersion = null,
      sourceInfo = null,
      javaVersion = null,
      msasImageVersion = null,
      databricksVersion = null
    )
    val text = configWithNulls.userAgentText

    assert(text != null && text.nonEmpty)
    assert(!text.contains("null"))
    assert(!text.contains("  "))
    assert(text.trim == text)
  }

  // Verify that Dataproc image tag (DATAPROC_IMAGE_VERSION) is included only when populated.
  test("UserAgentConfig conditionally includes Dataproc image version when populated") {
    val configWithImage = UserAgentConfig(
      connectorArtifactId = "spark-bigtable",
      connectorVersion = "0.10.1",
      sparkVersion = "3.5.1",
      scalaVersion = "2.12.18",
      sourceInfo = "DF/V1",
      msasImageVersion = "dataproc/3.0"
    )
    assert(configWithImage.userAgentText.contains("dataproc/3.0"))

    val configWithoutImage = UserAgentConfig(
      connectorArtifactId = "spark-bigtable",
      connectorVersion = "0.10.1",
      sparkVersion = "3.5.1",
      scalaVersion = "2.12.18",
      sourceInfo = "DF/V1",
      msasImageVersion = ""
    )
    assert(!configWithoutImage.userAgentText.contains("dataproc"))
  }

  // Verify that Databricks runtime tag (DATABRICKS_RUNTIME_VERSION) is included only when populated.
  test("UserAgentConfig conditionally includes Databricks runtime version when populated") {
    val configWithDbr = UserAgentConfig(
      connectorArtifactId = "spark-bigtable",
      connectorVersion = "0.10.1",
      sparkVersion = "3.5.1",
      scalaVersion = "2.12.18",
      sourceInfo = "DF/V1",
      databricksVersion = "databricks/18.3"
    )
    assert(configWithDbr.userAgentText.contains("databricks/18.3"))

    val configWithoutDbr = UserAgentConfig(
      connectorArtifactId = "spark-bigtable",
      connectorVersion = "0.10.1",
      sparkVersion = "3.5.1",
      scalaVersion = "2.12.18",
      sourceInfo = "DF/V1",
      databricksVersion = ""
    )
    assert(!configWithoutDbr.userAgentText.contains("databricks"))
  }

  // Verify that UserAgentConfig properly configures FixedHeaderProvider on BigtableDataSettings.
  test("UserAgentConfig applies user-agent header to BigtableDataSettings") {
    val config = UserAgentConfig(
      connectorArtifactId = "spark-bigtable",
      connectorVersion = "0.10.1",
      sparkVersion = "3.5.1",
      scalaVersion = "2.12.18",
      sourceInfo = "DF/V1",
      msasImageVersion = "dataproc/3.0"
    )

    val settingsBuilder = BigtableDataSettings.newBuilder()
      .setProjectId("test-project")
      .setInstanceId("test-instance")
    config.applySettings(settingsBuilder)

    val headerProvider = settingsBuilder.stubSettings().getHeaderProvider
    val headers = headerProvider.getHeaders
    assert(headers.containsKey(USER_AGENT_KEY.name()))
    val headerVal = headers.get(USER_AGENT_KEY.name())
    assert(headerVal != null && headerVal.contains("dataproc/3.0"))
  }

  // Verify that UserAgentConfig properly configures FixedHeaderProvider on BigtableTableAdminSettings.
  test("UserAgentConfig applies user-agent header to BigtableTableAdminSettings") {
    val config = UserAgentConfig(
      connectorArtifactId = "spark-bigtable",
      connectorVersion = "0.10.1",
      sparkVersion = "3.5.1",
      scalaVersion = "2.12.18",
      sourceInfo = "DF/V1",
      msasImageVersion = "dataproc/3.0"
    )

    val settingsBuilder = BigtableTableAdminSettings.newBuilder()
      .setProjectId("test-project")
      .setInstanceId("test-instance")
    config.applyTableAdminSettings(settingsBuilder)

    val headerProvider = settingsBuilder.stubSettings().getHeaderProvider
    val headers = headerProvider.getHeaders
    assert(headers.containsKey(USER_AGENT_KEY.name()))
    val headerVal = headers.get(USER_AGENT_KEY.name())
    assert(headerVal != null && headerVal.contains("dataproc/3.0"))
  }

  // Verify that BigtableSparkConfBuilder setters correctly populate UserAgentConfig fields and output.
  test("BigtableSparkConfBuilder configures UserAgentConfig without errors") {
    val conf = BigtableSparkConfBuilder()
      .setProjectId("test-project")
      .setInstanceId("test-instance")
      .setSparkVersion("3.5.1")
      .setScalaVersion("2.12.18")
      .setJavaVersion("17.0.10")
      .setUserAgentSourceInfo("DF/V1")
      .setMsasImageVersion("dataproc/3.0")
      .setDatabricksVersion("databricks/18.3")
      .build()

    val userAgentConfig = conf.bigtableClientConfig.userAgentConfig
    assert(userAgentConfig.sparkVersion == "3.5.1")
    assert(userAgentConfig.scalaVersion == "2.12.18")
    assert(userAgentConfig.javaVersion == "17.0.10")
    assert(userAgentConfig.msasImageVersion == "dataproc/3.0")
    assert(userAgentConfig.databricksVersion == "databricks/18.3")
    assert(userAgentConfig.userAgentText.contains("spark-bigtable/"))
    assert(userAgentConfig.userAgentText.contains("dataproc/3.0"))
    assert(userAgentConfig.userAgentText.contains("databricks/18.3"))
  }

  // Verify that companion object constants and system property defaults are defined and non-null.
  test("MSAS_IMAGE_VERSION, DATABRICKS_RUNTIME_VERSION, and JAVA_VERSION are defined") {
    assert(UserAgentConfig.MSAS_IMAGE_VERSION != null)
    assert(UserAgentConfig.DATABRICKS_RUNTIME_VERSION != null)
    assert(UserAgentConfig.JAVA_VERSION != null && UserAgentConfig.JAVA_VERSION.nonEmpty)
  }
}

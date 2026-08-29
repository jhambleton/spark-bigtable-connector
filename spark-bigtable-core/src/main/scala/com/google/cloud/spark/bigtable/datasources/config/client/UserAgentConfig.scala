package com.google.cloud.spark.bigtable.datasources.config.client

import com.google.api.gax.rpc.FixedHeaderProvider
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings
import com.google.cloud.bigtable.data.v2.BigtableDataSettings
import com.google.common.collect.ImmutableMap
import io.grpc.internal.GrpcUtil.USER_AGENT_KEY

object UserAgentConfig {
  private val CONNECTOR_VERSION = "0.10.1" // ${NEXT_VERSION_FLAG}
  private val CONNECTOR_ID = "spark-bigtable"

  // Managed Service for Apache Spark
  val MSAS_IMAGE_VERSION: String =
    Option(System.getenv("DATAPROC_IMAGE_VERSION"))
      .filter(_.nonEmpty)
      .map(image => s"dataproc/$image")
      .getOrElse("")

  // Databricks Runtime
  val DATABRICKS_RUNTIME_VERSION: String =
    Option(System.getenv("DATABRICKS_RUNTIME_VERSION"))
      .filter(_.nonEmpty)
      .map(dbr => s"databricks/$dbr")
      .getOrElse("")

  val JAVA_VERSION: String =
    Option(System.getProperty("java.version"))
      .getOrElse("UNSET_JAVA_VERSION")

  def apply(): UserAgentConfig = new UserAgentConfig(
    CONNECTOR_ID,
    CONNECTOR_VERSION,
    "UNSET_SPARK_VERSION",
    scala.util.Properties.versionNumberString,
    "UNSET_SOURCE",
    JAVA_VERSION,
    MSAS_IMAGE_VERSION,
    DATABRICKS_RUNTIME_VERSION
  )
}

case class UserAgentConfig(connectorArtifactId: String,
                           connectorVersion: String,
                           sparkVersion: String,
                           scalaVersion: String,
                           sourceInfo: String,
                           javaVersion: String = UserAgentConfig.JAVA_VERSION,
                           msasImageVersion: String = UserAgentConfig.MSAS_IMAGE_VERSION,
                           databricksVersion: String = UserAgentConfig.DATABRICKS_RUNTIME_VERSION) extends ClientConfigTrait {
  def this(connectorArtifactId: String,
           connectorVersion: String,
           sparkVersion: String,
           scalaVersion: String,
           sourceInfo: String) = this(
    connectorArtifactId,
    connectorVersion,
    sparkVersion,
    scalaVersion,
    sourceInfo,
    UserAgentConfig.JAVA_VERSION,
    UserAgentConfig.MSAS_IMAGE_VERSION,
    UserAgentConfig.DATABRICKS_RUNTIME_VERSION
  )

  def this(connectorArtifactId: String,
           connectorVersion: String,
           sparkVersion: String,
           scalaVersion: String,
           sourceInfo: String,
           msasImageVersion: String) = this(
    connectorArtifactId,
    connectorVersion,
    sparkVersion,
    scalaVersion,
    sourceInfo,
    UserAgentConfig.JAVA_VERSION,
    msasImageVersion,
    UserAgentConfig.DATABRICKS_RUNTIME_VERSION
  )

  override def getValidationErrors: Set[String] = Set()

  override def applySettings(settingsBuilder: BigtableDataSettings.Builder): Unit = {
    settingsBuilder
      .stubSettings()
      .setHeaderProvider(FixedHeaderProvider.create(
        ImmutableMap.of(USER_AGENT_KEY.name(), userAgentText)
      ))
  }

  def applyTableAdminSettings(settingsBuilder: BigtableTableAdminSettings.Builder): Unit = {
    settingsBuilder
      .stubSettings()
      .setHeaderProvider(FixedHeaderProvider.create(
        ImmutableMap.of(USER_AGENT_KEY.name(), userAgentText)
      ))
  }

  def userAgentText: String = {
    val base = f"$connectorArtifactId/$connectorVersion" +
      f" spark/$sparkVersion" +
      (if (javaVersion != null && javaVersion.nonEmpty) f" java/$javaVersion" else "") +
      (if (scalaVersion != null && scalaVersion.nonEmpty) f" scala/$scalaVersion" else "") +
      (if (sourceInfo != null && sourceInfo.nonEmpty) f" $sourceInfo" else "")
    val withMsas = if (msasImageVersion != null && msasImageVersion.nonEmpty) f"$base $msasImageVersion" else base
    if (databricksVersion != null && databricksVersion.nonEmpty) f"$withMsas $databricksVersion" else withMsas
  }

  override def debugString(): String =
    s"""UserAgentConfig(
       | connectorArtifactId: $connectorArtifactId
       | connectorVersion: $connectorVersion
       | sparkVersion: $sparkVersion
       | javaVersion: $javaVersion
       | scalaVersion: $scalaVersion
       | sourceInfo: $sourceInfo
       | msasImageVersion: $msasImageVersion
       | databricksVersion: $databricksVersion
       |)""".stripMargin
}

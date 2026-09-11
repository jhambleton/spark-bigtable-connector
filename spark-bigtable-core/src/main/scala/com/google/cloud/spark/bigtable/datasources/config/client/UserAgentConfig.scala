package com.google.cloud.spark.bigtable.datasources.config.client

import com.google.api.gax.rpc.FixedHeaderProvider
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings
import com.google.cloud.bigtable.data.v2.BigtableDataSettings
import com.google.common.collect.ImmutableMap
import io.grpc.internal.GrpcUtil.USER_AGENT_KEY

object UserAgentConfig {
  private val CONNECTOR_VERSION = "0.10.1" // ${NEXT_VERSION_FLAG}
  private val CONNECTOR_ID = "spark-bigtable"

  lazy val JAVA_VERSION: String =
    Option(System.getProperty("java.version"))
      .getOrElse("UNSET_JAVA_VERSION")

  // Priority-based platform and runtime detection (ensures at most one is ever set)
  lazy val DETECTED_PLATFORM_OR_RUNTIME: Option[UserAgentFlag] = detectPlatformOrRuntime()

  lazy val MSAS_IMAGE_VERSION: String = DETECTED_PLATFORM_OR_RUNTIME match {
    case Some(r: GcpRuntime) => r.flag
    case _ => ""
  }

  lazy val MSAS_SERVERLESS_VERSION: String = DETECTED_PLATFORM_OR_RUNTIME match {
    case Some(Platform.GcpServerless) => Platform.GcpServerless.flag
    case _ => ""
  }

  lazy val DATABRICKS_RUNTIME_VERSION: String = DETECTED_PLATFORM_OR_RUNTIME match {
    case Some(d: Platform.Databricks) => d.flag
    case _ => ""
  }

  lazy val K8S_ENVIRONMENT: String = DETECTED_PLATFORM_OR_RUNTIME match {
    case Some(p @ (Platform.GKE | Platform.EKS | Platform.K8s)) => p.flag
    case _ => ""
  }

  lazy val CUSTOM_PLATFORM_OR_RUNTIME: String = DETECTED_PLATFORM_OR_RUNTIME match {
    case Some(Platform.EMR) => Platform.EMR.flag
    case Some(Platform.EMRServerless) => Platform.EMRServerless.flag
    case _ => ""
  }

  private[client] def isRunningInKubernetes: Boolean =
    Option(System.getenv("KUBERNETES_SERVICE_HOST")).exists(_.trim.nonEmpty)

  private[client] def detectHostname(): Option[String] = {
    Option(System.getenv("HOSTNAME")).filter(_.nonEmpty)
  }

  private[client] def isMsasServerless(hostname: Option[String]): Boolean = {
    hostname.exists(_.startsWith("gdpic"))
  }

  private[client] def isEmrServerless(env: String => Option[String]): Boolean =
    env("PLATFORM_TYPE").contains("EMR_SERVERLESS") ||
      env("spark.master").exists(_.contains("emr-serverless")) ||
      env("SERVERLESS_EMR_JOB_ID").exists(_.nonEmpty)

  private[client] def isAwsPlatform(env: String => Option[String]): Boolean =
    env("spark.sql.emr.internal.extensions").exists(_.contains("EmrSparkSessionExtensions")) ||
      env("spark.emr.default.executor.cores").exists(_.nonEmpty) ||
      isEmrServerless(env)

  private[client] def detectAwsPlatform(
      env: String => Option[String],
      isK8s: Boolean
  ): Option[Platform] = {
    if (isAwsPlatform(env)) {
      if (isEmrServerless(env)) {
        Some(Platform.EMRServerless)
      } else if (isK8s) {
        Some(Platform.EKS)
      } else {
        Some(Platform.EMR)
      }
    } else {
      None
    }
  }

  private[client] def isGkeEnvironment(env: String => Option[String]): Boolean = {
    // 1. Injected by Dataproc on GKE runtime
    env("DATAPROC_DIR").exists(_.nonEmpty) ||
    // 2. Dataproc on GKE container image
    env("spark.kubernetes.container.image").exists(img =>
      img.contains("dataproc") || img.contains("cloud-dataproc"))
  }

  private[client] def detectGcpPlatform(
      env: String => Option[String],
      hostname: Option[String],
      isK8s: Boolean
  ): Option[UserAgentFlag] = {
    // 1. Dataproc Serverless (hostname starts with gdpic)
    if (isMsasServerless(hostname)) {
      Some(Platform.GcpServerless)
    } else {
      // 2. Dataproc on GCE (DATAPROC_IMAGE_VERSION set and not gdpic)
      env("DATAPROC_IMAGE_VERSION").filter(_.nonEmpty) match {
        case Some(image) => Some(GcpRuntime.Dataproc(image))
        case None =>
          // 3. Dataproc on GKE
          if (isK8s && isGkeEnvironment(env)) {
            Some(Platform.GKE)
          } else {
            None
          }
      }
    }
  }

  /**
   * Priority chain for platform/runtime detection.
   * Runs checks progressively and escapes as soon as a platform is identified:
   * 1. GCP Platforms (Dataproc Serverless, Dataproc on GCE, Dataproc on GKE)
   * 2. AWS Platforms (EMR Serverless, EMR on EKS, EMR on EC2)
   * 3. Databricks (DATABRICKS_RUNTIME_VERSION)
   * 4. Generic Kubernetes (KUBERNETES_SERVICE_HOST or service account)
   */
  private[client] def detectPlatformOrRuntime(
      env: String => Option[String],
      hostname: Option[String],
      isK8s: Boolean
  ): Option[UserAgentFlag] = {
    // Progressively check GCP platforms first. If identified, escape immediately.
    detectGcpPlatform(env, hostname, isK8s)
      .orElse(detectAwsPlatform(env, isK8s))
      .orElse {
        env("DATABRICKS_RUNTIME_VERSION").filter(_.nonEmpty)
          .map(Platform.Databricks)
          .orElse(if (isK8s) Some(Platform.K8s) else None)
      }
  }

  private[client] def detectPlatformOrRuntime(): Option[UserAgentFlag] = {
    detectPlatformOrRuntime(
      k => Option(System.getenv(k)).orElse(Option(System.getProperty(k))),
      detectHostname(),
      isRunningInKubernetes
    )
  }

  def apply(): UserAgentConfig = new UserAgentConfig(
    CONNECTOR_ID,
    CONNECTOR_VERSION,
    "UNSET_SPARK_VERSION",
    scala.util.Properties.versionNumberString,
    "UNSET_SOURCE",
    JAVA_VERSION,
    MSAS_IMAGE_VERSION,
    DATABRICKS_RUNTIME_VERSION,
    K8S_ENVIRONMENT,
    MSAS_SERVERLESS_VERSION,
    CUSTOM_PLATFORM_OR_RUNTIME
  )
}

case class UserAgentConfig(connectorArtifactId: String,
                           connectorVersion: String,
                           sparkVersion: String,
                           scalaVersion: String,
                           sourceInfo: String,
                           javaVersion: String = UserAgentConfig.JAVA_VERSION,
                           msasImageVersion: String = UserAgentConfig.MSAS_IMAGE_VERSION,
                           databricksVersion: String = UserAgentConfig.DATABRICKS_RUNTIME_VERSION,
                           k8sEnvironment: String = UserAgentConfig.K8S_ENVIRONMENT,
                           msasServerlessVersion: String = UserAgentConfig.MSAS_SERVERLESS_VERSION,
                           customPlatformOrRuntime: String = UserAgentConfig.CUSTOM_PLATFORM_OR_RUNTIME) extends ClientConfigTrait {
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
    UserAgentConfig.DATABRICKS_RUNTIME_VERSION,
    UserAgentConfig.K8S_ENVIRONMENT,
    UserAgentConfig.MSAS_SERVERLESS_VERSION,
    UserAgentConfig.CUSTOM_PLATFORM_OR_RUNTIME
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
    UserAgentConfig.DATABRICKS_RUNTIME_VERSION,
    UserAgentConfig.K8S_ENVIRONMENT,
    UserAgentConfig.MSAS_SERVERLESS_VERSION,
    UserAgentConfig.CUSTOM_PLATFORM_OR_RUNTIME
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
    Seq(
      Some(s"$connectorArtifactId/$connectorVersion"),
      Some(s"spark/$sparkVersion"),
      Option(javaVersion).filter(_.nonEmpty).map(v => s"java/$v"),
      Option(scalaVersion).filter(_.nonEmpty).map(v => s"scala/$v"),
      Option(sourceInfo).filter(_.nonEmpty),
      Option(msasImageVersion).filter(_.nonEmpty),
      Option(databricksVersion).filter(_.nonEmpty),
      Option(k8sEnvironment).filter(_.nonEmpty),
      Option(msasServerlessVersion).filter(_.nonEmpty),
      Option(customPlatformOrRuntime).filter(_.nonEmpty)
    ).flatten.mkString(" ")
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
       | k8sEnvironment: $k8sEnvironment
       | msasServerlessVersion: $msasServerlessVersion
       | customPlatformOrRuntime: $customPlatformOrRuntime
       |)""".stripMargin
}

/**
 * Type definitions for platform and runtime user-agent flags.
 *
 * GCP runtimes track runtime/version (dataproc/<version>, serverless/<version>),
 * while other platforms emit platform/<platform-name> (platform/gke, platform/eks, platform/emr, platform/emr-serverless, platform/k8s).
 */
sealed trait UserAgentFlag {
  def flag: String
}

// Category 1: GCP Managed Spark Runtimes (format: <runtime>/<version>)
sealed trait GcpRuntime extends UserAgentFlag

object GcpRuntime {
  case class Dataproc(version: String) extends GcpRuntime {
    override def flag: String = s"dataproc/$version"
  }
}

// Category 2: Platforms, Cloud Orchestrators & Runtimes (format: platform/<platform-name>)
sealed abstract class Platform(val flag: String) extends UserAgentFlag

object Platform {
  case object GcpServerless extends Platform("platform/gcp-serverless")
  case object GKE extends Platform("platform/gke")
  case object EKS extends Platform("platform/eks")
  case class Databricks(version: String) extends Platform(
    Option(version).map(_.trim).filter(_.nonEmpty).map(v => s"databricks/$v").getOrElse("databricks")
  )
  case object EMR extends Platform("platform/emr")
  case object EMRServerless extends Platform("platform/emr-serverless")
  case object K8s extends Platform("platform/k8s")
  case object NoPlatform extends Platform("")
}

object UserAgentFlag {
  type GcpRuntime = com.google.cloud.spark.bigtable.datasources.config.client.GcpRuntime
  val Dataproc = GcpRuntime.Dataproc

  type OtherPlatform = Platform
  val GcpServerless = Platform.GcpServerless
  val Gke = Platform.GKE
  val Eks = Platform.EKS
  val Emr = Platform.EMR
  val EmrServerless = Platform.EMRServerless
  val K8s = Platform.K8s
  val Databricks = Platform.Databricks
}



package com.google.cloud.spark.bigtable.datasources.config.client

import com.google.api.gax.rpc.FixedHeaderProvider
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings
import com.google.cloud.bigtable.data.v2.BigtableDataSettings
import com.google.common.collect.ImmutableMap
import io.grpc.internal.GrpcUtil.USER_AGENT_KEY

object UserAgentConfig {
  private val CONNECTOR_VERSION = "0.10.1" // ${NEXT_VERSION_FLAG}
  private val CONNECTOR_ID = "spark-bigtable"

  val JAVA_VERSION: String =
    Option(System.getProperty("java.version"))
      .getOrElse("UNSET_JAVA_VERSION")

  // Priority-based platform and runtime detection (ensures at most one is ever set)
  val DETECTED_PLATFORM_OR_RUNTIME: Option[UserAgentFlag] = detectPlatformOrRuntime()

  val MSAS_IMAGE_VERSION: String = DETECTED_PLATFORM_OR_RUNTIME match {
    case Some(GcpRuntime.Dataproc(version)) => s"dataproc/$version"
    case _ => ""
  }

  val MSAS_SERVERLESS_VERSION: String = DETECTED_PLATFORM_OR_RUNTIME match {
    case Some(Platform.GcpServerless) => Platform.GcpServerless.flag
    case _ => ""
  }

  val DATABRICKS_RUNTIME_VERSION: String = DETECTED_PLATFORM_OR_RUNTIME match {
    case Some(Platform.Databricks(version)) => s"databricks/$version"
    case _ => ""
  }

  val K8S_ENVIRONMENT: String = DETECTED_PLATFORM_OR_RUNTIME match {
    case Some(p: Platform) if p == Platform.GKE || p == Platform.EKS || p == Platform.AKS || p == Platform.K8s => p.flag
    case _ => ""
  }

  val CUSTOM_PLATFORM_OR_RUNTIME: String = DETECTED_PLATFORM_OR_RUNTIME match {
    case Some(Platform.EMR) => Platform.EMR.flag
    case Some(Platform.EMRServerless) => Platform.EMRServerless.flag
    case Some(Platform.Synapse) => Platform.Synapse.flag
    case _ => ""
  }

  private[client] def isRunningInKubernetes: Boolean = {
    val apiHost = System.getenv("KUBERNETES_SERVICE_HOST")
    if (apiHost != null && apiHost.trim.nonEmpty) {
      return true
    }
    val saFolder = new java.io.File("/var/run/secrets/kubernetes.io/serviceaccount")
    saFolder.exists() && saFolder.isDirectory
  }

  private[client] def isKubernetes: Boolean = isRunningInKubernetes

  private[client] def detectHostname(): Option[String] = {
    Option(System.getenv("HOSTNAME"))
      .filter(_.nonEmpty)
      .orElse(try {
        Option(java.net.InetAddress.getLocalHost.getHostName).filter(_.nonEmpty)
      } catch {
        case _: Throwable => None
      })
  }

  private[client] def isMsasServerless: Boolean = isMsasServerless(detectHostname())

  private[client] def isMsasServerless(hostname: Option[String]): Boolean = {
    hostname.exists(_.startsWith("gdpic"))
  }

  private[client] def isEmrServerless(env: String => Option[String]): Boolean =
    env("AWS_EMR_SERVERLESS_JOB_RUN_ID").exists(_.nonEmpty) ||
      env("AWS_EMR_SERVERLESS_APPLICATION_ID").exists(_.nonEmpty)

  private[client] def isEmr(env: String => Option[String]): Boolean =
    env("EMR_CLUSTER_ID").exists(_.nonEmpty) ||
      env("AWS_EMR_JOB_FLOW_ID").exists(_.nonEmpty)

  private[client] def isSynapse(env: String => Option[String]): Boolean =
    env("SYNAPSE_WORKSPACE_NAME").exists(_.nonEmpty) ||
      env("AZURE_SERVICE_NAME").exists(_.equalsIgnoreCase("synapse"))

  private[client] def isGkeEnvironment(env: String => Option[String]): Boolean = {
    // 1. Injected by Dataproc on GKE runtime
    env("DATAPROC_DIR").exists(_.nonEmpty) ||
    // 2. Spark Kubernetes container image hosted on Google Artifact Registry / GCR
    env("spark.kubernetes.container.image").exists(img =>
      img.contains("cloud-dataproc") || img.contains("pkg.dev") || img.contains("gcr.io"))
  }

  private[client] def detectK8sPlatform(env: String => Option[String], isK8s: Boolean): Option[Platform] = {
    if (!isK8s) {
      None
    } else if (isGkeEnvironment(env)) {
      Some(Platform.GKE)
    } else {
      Some(Platform.K8s)
    }
  }

  /**
   * Priority chain for platform/runtime detection.
   * Only one platform or runtime is ever detected:
   * 1. Dataproc on GCE (DATAPROC_IMAGE_VERSION set, hostname not gdpic)
   * 2. Dataproc Serverless (hostname starts with gdpic)
   * 3. EMR Serverless (AWS_EMR_SERVERLESS_JOB_RUN_ID set)
   * 4. EMR on EC2 (EMR_CLUSTER_ID set)
   * 5. Databricks (DATABRICKS_RUNTIME_VERSION set)
   * 6. Azure Synapse (SYNAPSE_WORKSPACE_NAME set)
   * 7. Kubernetes (KUBERNETES_SERVICE_HOST set)
   */
  private[client] def detectPlatformOrRuntime(
      env: String => Option[String],
      hostname: Option[String],
      isK8s: Boolean
  ): Option[UserAgentFlag] = {
    val msasImage = env("DATAPROC_IMAGE_VERSION").filter(_.nonEmpty)
    val isServerless = isMsasServerless(hostname)

    if (msasImage.isDefined && !isServerless) {
      Some(GcpRuntime.Dataproc(msasImage.get))
    } else if (isServerless) {
      Some(Platform.GcpServerless)
    } else if (isEmrServerless(env)) {
      Some(Platform.EMRServerless)
    } else if (isEmr(env)) {
      Some(Platform.EMR)
    } else if (env("DATABRICKS_RUNTIME_VERSION").exists(_.nonEmpty)) {
      Some(Platform.Databricks(env("DATABRICKS_RUNTIME_VERSION").get))
    } else if (isSynapse(env)) {
      Some(Platform.Synapse)
    } else if (isK8s) {
      detectK8sPlatform(env, isK8s)
    } else {
      None
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
    val base: String = f"$connectorArtifactId/$connectorVersion" +
      f" spark/$sparkVersion" +
      (if (javaVersion != null && javaVersion.nonEmpty) f" java/$javaVersion" else "") +
      (if (scalaVersion != null && scalaVersion.nonEmpty) f" scala/$scalaVersion" else "") +
      (if (sourceInfo != null && sourceInfo.nonEmpty) f" $sourceInfo" else "")
    val withMsas: String = if (msasImageVersion != null && msasImageVersion.nonEmpty) f"$base $msasImageVersion" else base
    val withDatabricks: String = if (databricksVersion != null && databricksVersion.nonEmpty) f"$withMsas $databricksVersion" else withMsas
    val withK8s: String = if (k8sEnvironment != null && k8sEnvironment.nonEmpty) f"$withDatabricks $k8sEnvironment" else withDatabricks
    val withServerless: String = if (msasServerlessVersion != null && msasServerlessVersion.nonEmpty) f"$withK8s $msasServerlessVersion" else withK8s
    if (customPlatformOrRuntime != null && customPlatformOrRuntime.nonEmpty) f"$withServerless $customPlatformOrRuntime" else withServerless
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
 * while other platforms emit platform/<platform-name> (platform/gke, platform/eks, platform/emr, platform/emr-serverless, platform/synapse, platform/aks, platform/k8s).
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
  case object AKS extends Platform("platform/aks")
  case class Databricks(version: String) extends Platform(
    if (version != null && version.trim.nonEmpty) s"databricks/${version.trim}" else "databricks"
  )
  case object EMR extends Platform("platform/emr")
  case object EMRServerless extends Platform("platform/emr-serverless")
  case object Synapse extends Platform("platform/synapse")
  case object K8s extends Platform("platform/k8s")
  case object None extends Platform("")
}

object UserAgentFlag {
  type GcpRuntime = com.google.cloud.spark.bigtable.datasources.config.client.GcpRuntime
  val Dataproc = GcpRuntime.Dataproc

  type OtherPlatform = Platform
  val GcpServerless = Platform.GcpServerless
  val Gke = Platform.GKE
  val Eks = Platform.EKS
  val Aks = Platform.AKS
  val Emr = Platform.EMR
  val EmrServerless = Platform.EMRServerless
  val Synapse = Platform.Synapse
  val K8s = Platform.K8s
  val Databricks = Platform.Databricks
}



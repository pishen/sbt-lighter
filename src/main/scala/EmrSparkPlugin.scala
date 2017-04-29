/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sbtemrspark

import scala.collection.JavaConverters._

import com.amazonaws.services.elasticmapreduce.{AmazonElasticMapReduce, AmazonElasticMapReduceClientBuilder}
import com.amazonaws.services.elasticmapreduce.model.{Unit => _, Configuration => EMRConfiguration, _}
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import play.api.libs.json._
import sbinary.DefaultProtocol.StringFormat
import sbt._
import sbt.Cache.seqFormat
import sbt.Defaults.runMainParser
import sbt.Keys._
import sbt.complete.DefaultParsers._
import sbtassembly.AssemblyKeys._
import sbtassembly.AssemblyPlugin
import java.util.Collection
import scala.util.Try

object EmrSparkPlugin extends AutoPlugin {
  object autoImport {
    //configs
    val sparkClusterName = settingKey[String]("emr cluster's name")
    val sparkAwsRegion = settingKey[String]("aws's region")
    val sparkEmrRelease = settingKey[String]("emr's release label")
    val sparkEmrServiceRole = settingKey[String]("emr's service role")
    val sparkEmrAutoScalingRole = settingKey[Option[String]]("emr Auto scaling role")
    val sparkKeyName = settingKey[Option[String]]("Key pair name")
    val sparkSubnetId = settingKey[Option[String]]("spark's subnet id")
    val sparkInstanceCount = settingKey[Int]("total number of instances")
    val sparkInstanceType = settingKey[String]("spark nodes' instance type")
    val sparkInstanceBidPrice = settingKey[Option[String]]("spark nodes' bid price")
    val sparkInstanceRole = settingKey[String]("spark ec2 instance's role")
    val sparkTags = settingKey[Option[Map[String, String]]]("spark ec2 instance's tags")
    val sparkEmrManagedMasterSecurityGroup = settingKey[Option[String]]("EMR managed security group ids for the master ec2 instance")
    val sparkEmrManagedSlaveSecurityGroup = settingKey[Option[String]]("EMR security group ids for the slave ec2 instances")
    val sparkAdditionalMasterSecurityGroups = settingKey[Option[Seq[String]]]("additional security group ids for the master ec2 instance")
    val sparkAdditionalSlaveSecurityGroups = settingKey[Option[Seq[String]]]("additional security group ids for the slave ec2 instances")
    val sparkS3JarFolder = settingKey[String]("S3 folder for putting the executable jar")
    val sparkS3LoggingFolder = settingKey[Option[String]]("S3 folder for application's logs")
    val sparkS3JsonConfiguration = settingKey[Option[String]]("S3 location for the EMR cluster json configuration")
    val sparkAdditionalApplications = settingKey[Option[Seq[String]]]("Applications other than Spark to be deployed on the EMR cluster, these are case insensitive.")
    val sparkEmrSteps = settingKey[Option[Seq[StepConfig]]]("Multiple steps to run once cluster is setup")
    val sparkSettings = settingKey[Settings]("wrapper object for above settings")

    //commands
    val sparkCreateCluster = taskKey[Unit]("create cluster")
    val sparkListClusters = taskKey[Unit]("list existing active clusters")
    val sparkTerminateCluster = taskKey[Unit]("terminate cluster")
    val sparkSubmitJob = inputKey[Unit]("submit the job")
    val sparkSubmitJobWithMain = inputKey[Unit]("submit the job with specified main class")
    val sparkSubmitSteps = taskKey[Unit]("submit multiple steps")
    val sparkUploadJarToS3 = taskKey[Unit]("Upload jar to S3")
  }
  import autoImport._

  override def trigger = allRequirements
  override def requires = AssemblyPlugin

  val activatedClusterStates = Seq(ClusterState.RUNNING, ClusterState.STARTING, ClusterState.WAITING, ClusterState.BOOTSTRAPPING)

  case class Settings(
    clusterName: String,
    awsRegion: String,
    emrRelease: String,
    emrServiceRole: String,
    emrAutoScalingRole: Option[String],
    keyName: Option[String],
    subnetId: Option[String],
    tags: Option[Map[String, String]],
    instanceCount: Int,
    instanceType: String,
    instanceBidPrice: Option[String],
    instanceRole: String,
    emrManagedMasterSecurityGroup: Option[String],
    emrManagedSlaveSecurityGroup: Option[String],
    additionalMasterSecurityGroups: Option[Seq[String]],
    additionalSlaveSecurityGroups: Option[Seq[String]],
    s3JarFolder: String,
    s3LoggingFolder: Option[String],
    s3JsonConfiguration: Option[String],
    additionalApplications: Option[Seq[String]]
  )

  override lazy val projectSettings = Seq(
    sparkClusterName := name.value,
    sparkEmrRelease := "emr-5.4.0",
    sparkEmrServiceRole := "EMR_DefaultRole",
    sparkEmrAutoScalingRole := None,
    sparkKeyName := None,
    sparkSubnetId := None,
    sparkInstanceCount := 1,
    sparkInstanceType := "m3.xlarge",
    sparkInstanceBidPrice := None,
    sparkInstanceRole := "EMR_EC2_DefaultRole",
    sparkTags := None,
    sparkEmrManagedMasterSecurityGroup := None,
    sparkEmrManagedSlaveSecurityGroup := None,
    sparkAdditionalMasterSecurityGroups := None,
    sparkAdditionalSlaveSecurityGroups := None,
    sparkS3LoggingFolder := None,
    sparkS3JsonConfiguration := None,
    sparkAdditionalApplications := None,
    sparkEmrSteps := None,

    sparkSettings := Settings(
      sparkClusterName.value,
      sparkAwsRegion.value,
      sparkEmrRelease.value,
      sparkEmrServiceRole.value,
      sparkEmrAutoScalingRole.value,
      sparkKeyName.value,
      sparkSubnetId.value,
      sparkTags.value,
      sparkInstanceCount.value,
      sparkInstanceType.value,
      sparkInstanceBidPrice.value,
      sparkInstanceRole.value,
      sparkEmrManagedMasterSecurityGroup.value,
      sparkEmrManagedSlaveSecurityGroup.value,
      sparkAdditionalMasterSecurityGroups.value,
      sparkAdditionalSlaveSecurityGroups.value,
      sparkS3JarFolder.value,
      sparkS3LoggingFolder.value,
      sparkS3JsonConfiguration.value,
      sparkAdditionalApplications.value
    ),

    sparkCreateCluster := {
      implicit val log = streams.value.log
      log.debug("Starting EMR cluster with config \n " + sparkSettings.value)
      createCluster(sparkSettings.value, None)
    },

    sparkSubmitJob := {
      implicit val log = streams.value.log
      val args = spaceDelimited("<arg>").parsed
      val mainClassValue = (mainClass in Compile).value.getOrElse(sys.error("Can't locate the main class in your application."))
      submitJob(sparkSettings.value, mainClassValue, args, assembly.value)
    },

    sparkSubmitJobWithMain := {
      Def.inputTask {
        implicit val log = streams.value.log
        val (mainClass, args) = loadForParser(discoveredMainClasses in Compile)((s, names) => runMainParser(s, names getOrElse Nil)).parsed
        submitJob(sparkSettings.value, mainClass, args, assembly.value)
      }.evaluated
    },

    sparkSubmitSteps := {
      implicit val log = streams.value.log
      sparkEmrSteps.value match {
        case Some(steps) => submitSteps(sparkSettings.value, steps)
        case None => sys.error("Steps not defined for sparkSubmitSteps task.")
      }
    },

    sparkUploadJarToS3 := {
      implicit val log = streams.value.log
      if (Try(uploadJarToS3(sparkSettings.value.s3JarFolder, assembly.value)).isFailure)
        sys.error("Failed to upload application jar to S3.")
    },

    sparkTerminateCluster := {
      val log = streams.value.log

      val emr = buildEmr(sparkSettings.value)
      val clusterIdOpt = emr
        .listClusters(new ListClustersRequest().withClusterStates(activatedClusterStates: _*))
        .getClusters().asScala
        .find(_.getName == sparkClusterName.value)
        .map(_.getId)

      clusterIdOpt match {
        case None =>
          log.info(s"The cluster with name ${sparkClusterName.value} does not exist.")
        case Some(clusterId) =>
          emr.terminateJobFlows(new TerminateJobFlowsRequest().withJobFlowIds(clusterId))
          log.info(s"Cluster with id $clusterId is terminating, please check aws console for the following information.")
      }
    },

    sparkListClusters := {
      val log = streams.value.log

      val emr = buildEmr(sparkSettings.value)
      val clusters = emr
        .listClusters(new ListClustersRequest().withClusterStates(activatedClusterStates: _*))
        .getClusters().asScala

      if (clusters.isEmpty) {
        log.info("No active cluster found.")
      } else {
        log.info(s"${clusters.length} active clusters found: ")
        clusters.foreach { c =>
          log.info(s"Name: ${c.getName} | Id: ${c.getId}")
        }
      }
    }
  )

  def createCluster(settings: Settings, stepConfig: Option[Collection[StepConfig]])(implicit log: Logger) = {
    val emr = buildEmr(settings)
    val clustersNames = emr
      .listClusters(new ListClustersRequest().withClusterStates(activatedClusterStates: _*))
      .getClusters().asScala
      .map(_.getName)

    if (clustersNames.exists(_ == settings.clusterName)) {
      log.error(s"A cluster with name ${settings.clusterName} already exists.")
    } else {
      val request = Some(new RunJobFlowRequest())
        .map(r => settings.s3LoggingFolder.fold(r)(folder => r.withLogUri(folder)))
        .map(r => stepConfig.fold(r)(c => r.withSteps(c)))
        .map(r => settings.emrAutoScalingRole.fold(r)(c => r.withAutoScalingRole(c)))
        .map(r => settings.s3JsonConfiguration.fold(r) { url =>
          log.info(s"Importing configuration from $url")
          val s3 = AmazonS3ClientBuilder.defaultClient()
          val s3Url = new S3Url(url)
          val json = Json.parse(s3.getObject(s3Url.bucket, s3Url.key).getObjectContent)
          def parseConfigurations(json: JsValue): Seq[EMRConfiguration] = {
            json.as[Seq[JsObject]].map { obj =>
              Some(new EMRConfiguration())
                .map { conf =>
                  (obj \ "Properties").asOpt[Map[String, String]]
                    .filter(_.nonEmpty)
                    .fold(conf)(props => conf.withProperties(props.asJava))
                }
                .map { conf =>
                  (obj \ "Configurations").asOpt[JsValue]
                    .map(json => parseConfigurations(json))
                    .filter(_.nonEmpty)
                    .fold(conf)(confs => conf.withConfigurations(confs: _*))
                }
                .get
                .withClassification((obj \ "Classification").as[String])
            }
          }
          r.withConfigurations(parseConfigurations(json): _*)
        })
        .map(r => settings.tags.fold(r)(tags => r.withTags(tags.map { case (k, v) => new Tag(k, v) }.asJavaCollection)))
        .get
        .withName(settings.clusterName)
        .withApplications(("Spark" +: settings.additionalApplications.getOrElse(Seq.empty)).map(app => new Application().withName(app)): _*)
        .withReleaseLabel(settings.emrRelease)
        .withServiceRole(settings.emrServiceRole)
        .withJobFlowRole(settings.instanceRole)
        .withInstances {
          Some(new JobFlowInstancesConfig())
            .map(c => settings.subnetId.fold(c)(id => c.withEc2SubnetId(id)))
            .map(c => settings.keyName.fold(c)(key => c.withEc2KeyName(key)))
            .map(c => settings.emrManagedMasterSecurityGroup.fold(c)(c.withEmrManagedMasterSecurityGroup))
            .map(c => settings.emrManagedSlaveSecurityGroup.fold(c)(c.withEmrManagedSlaveSecurityGroup))
            .map(c => settings.additionalMasterSecurityGroups.fold(c)(ids => c.withAdditionalMasterSecurityGroups(ids: _*)))
            .map(c => settings.additionalSlaveSecurityGroups.fold(c)(ids => c.withAdditionalSlaveSecurityGroups(ids: _*)))
            .get
            .withInstanceGroups {
              val masterConfig = Some(new InstanceGroupConfig())
                .map(c => settings.instanceBidPrice.fold(c.withMarket(MarketType.ON_DEMAND))(c.withMarket(MarketType.SPOT).withBidPrice))
                .get
                .withInstanceCount(1)
                .withInstanceRole(InstanceRoleType.MASTER)
                .withInstanceType(settings.instanceType)

              val slaveCount = settings.instanceCount - 1
              val slaveConfig = Some(new InstanceGroupConfig())
                .map(c => settings.instanceBidPrice.fold(c.withMarket(MarketType.ON_DEMAND))(c.withMarket(MarketType.SPOT).withBidPrice))
                .get
                .withInstanceCount(slaveCount)
                .withInstanceRole(InstanceRoleType.CORE)
                .withInstanceType(settings.instanceType)

              if (slaveCount <= 0) {
                Seq(masterConfig).asJava
              } else {
                Seq(masterConfig, slaveConfig).asJava
              }
            }
            .withKeepJobFlowAliveWhenNoSteps(stepConfig.isEmpty)
        }
      val res = emr.runJobFlow(request)
      log.info(s"Your new cluster's id is ${res.getJobFlowId}, you may check its status on AWS console.")
    }
  }

  def uploadJarToS3(s3Location: String, jarFile: File)(implicit log: Logger): S3Url = {
    log.info(s"Uploading jar ${jarFile.getName} to S3 path $s3Location")
    val s3 = AmazonS3ClientBuilder.defaultClient()
    val jarUrl = new S3Url(s3Location) / jarFile.getName
    val startTime = System.currentTimeMillis
    s3.putObject(jarUrl.bucket, jarUrl.key, jarFile)
    log.info(s"Jar uploaded in ${(System.currentTimeMillis - startTime) / 1000} secs")
    jarUrl
  }

  def getClusterId(settings: Settings): Option[String] = {
    buildEmr(settings)
      .listClusters(new ListClustersRequest().withClusterStates(activatedClusterStates: _*))
      .getClusters().asScala
      .find(_.getName == settings.clusterName)
      .map(_.getId)
  }

  def submitJob(
    settings: Settings,
    mainClass: String,
    args: Seq[String],
    jar: File
  )(implicit log: Logger) = {

    val s3Location = settings.s3JarFolder
    val uploadedAt = uploadJarToS3(s3Location, jar)
    val clusterIdOpt = getClusterId(settings)
    val emr = buildEmr(settings)
    //submit job
    val stepConfig = new StepConfig()
      .withActionOnFailure(ActionOnFailure.CONTINUE)
      .withName("Spark Step")
      .withHadoopJarStep(
        new HadoopJarStepConfig()
          .withJar("command-runner.jar")
          .withArgs((Seq("spark-submit", "--deploy-mode", "cluster", "--class", mainClass, uploadedAt.toString) ++ args).asJava)
      )
    clusterIdOpt match {
      case Some(clusterId) =>
        val res = emr.addJobFlowSteps(
          new AddJobFlowStepsRequest()
            .withJobFlowId(clusterId)
            .withSteps(stepConfig)
        )
        log.info(s"Job submitted with job id ${res.getStepIds.asScala.mkString(",")}")
      case None =>
        createCluster(settings, Some(List(stepConfig).asJava))
    }
  }

  def submitSteps(
    settings: Settings,
    steps: Seq[StepConfig]
  )(implicit log: Logger) = {

    getClusterId(settings) match {
      case Some(clusterId) =>
        val res = buildEmr(settings).addJobFlowSteps(
          new AddJobFlowStepsRequest()
            .withJobFlowId(clusterId)
            .withSteps(steps.asJavaCollection)
        )
        log.info(s"Job submitted with job id ${res.getStepIds.asScala.mkString(",")}")
      case None =>
        log.warn("Couldn't find cluster. Will create one before submitting")
        createCluster(settings, Some(steps.asJavaCollection))
    }
  }

  def buildEmr(settings: Settings) = {
    AmazonElasticMapReduceClientBuilder.standard()
      .withRegion(settings.awsRegion)
      .build()
  }

  class S3Url(url: String) {
    require(url.startsWith("s3://"), "S3 url should starts with \"s3://\".")

    val (bucket, key) = url.drop(5).split("/").toList match {
      case head :: Nil => (head, "")
      case head :: tail => (head, tail.mkString("/"))
      case _ => sys.error(s"unrecognized s3 url: $url")
    }

    def /(subPath: String) = {
      val newKey = if (key == "") subPath else key + "/" + subPath
      new S3Url(s"s3://$bucket/$newKey")
    }

    override def toString = s"s3://$bucket/$key"
  }
}

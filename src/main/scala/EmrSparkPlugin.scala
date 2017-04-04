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

import scala.collection.JavaConverters._
import com.amazonaws.services.elasticmapreduce.{AmazonElasticMapReduce, AmazonElasticMapReduceClientBuilder}
import com.amazonaws.services.elasticmapreduce.model.{Unit => _, _}
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import sbinary.DefaultProtocol.StringFormat
import sbt._
import sbt.Cache.seqFormat
import sbt.Defaults.runMainParser
import sbt.Keys._
import sbt.complete.DefaultParsers._
import sbtassembly.AssemblyKeys._
import sbtassembly.AssemblyPlugin

object EmrSparkPlugin extends AutoPlugin {
  object autoImport {
    //configs
    val sparkClusterName = settingKey[String]("emr cluster's name")
    val sparkAwsRegion = settingKey[String]("aws's region")
    val sparkEmrRelease = settingKey[String]("emr's release label")
    val sparkEmrServiceRole = settingKey[String]("emr's service role")
    val sparkSubnetId = settingKey[Option[String]]("spark's subnet id")
    val sparkInstanceCount = settingKey[Int]("total number of instances")
    val sparkInstanceType = settingKey[String]("spark nodes' instance type")
    val sparkInstanceBidPrice = settingKey[Option[String]]("spark nodes' bid price")
    val sparkInstanceRole = settingKey[String]("spark ec2 instance's role")
    val sparkAdditionalSecurityGroupIds = settingKey[Option[Seq[String]]]("additional security group ids for the ec2")
    val sparkS3JarFolder = settingKey[String]("S3 folder for putting the executable jar")
    val sparkS3LoggingFolder = settingKey[Option[String]]("S3 folder for application's logs")
    val sparkSettings = settingKey[Settings]("wrapper object for above settings")
    //commands
    val sparkCreateCluster = taskKey[Unit]("create cluster")
    val sparkListClusters = taskKey[Unit]("list existing active clusters")
    val sparkTerminateCluster = taskKey[Unit]("terminate cluster")
    val sparkSubmitJob = inputKey[Unit]("submit the job")
    val sparkSubmitJobWithMain = inputKey[Unit]("submit the job with specified main class")
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
    subnetId: Option[String],
    instanceCount: Int,
    instanceType: String,
    instanceBidPrice: Option[String],
    instanceRole: String,
    additionalSecurityGroupIds: Option[Seq[String]],
    s3JarFolder: String,
    s3LoggingFolder: Option[String]
  )

  override lazy val projectSettings = Seq(
    sparkClusterName := name.value,
    sparkEmrRelease := "emr-5.4.0",
    sparkEmrServiceRole := "EMR_DefaultRole",
    sparkSubnetId := None,
    sparkInstanceCount := 1,
    sparkInstanceType := "m3.xlarge",
    sparkInstanceBidPrice := None,
    sparkInstanceRole := "EMR_EC2_DefaultRole",
    sparkAdditionalSecurityGroupIds := None,
    sparkS3LoggingFolder := None,

    sparkSettings := Settings(
      sparkClusterName.value,
      sparkAwsRegion.value,
      sparkEmrRelease.value,
      sparkEmrServiceRole.value,
      sparkSubnetId.value,
      sparkInstanceCount.value,
      sparkInstanceType.value,
      sparkInstanceBidPrice.value,
      sparkInstanceRole.value,
      sparkAdditionalSecurityGroupIds.value,
      sparkS3JarFolder.value,
      sparkS3LoggingFolder.value
    ),

    sparkCreateCluster := {
      implicit val log = streams.value.log
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

  def createCluster(settings: Settings, stepConfig: Option[StepConfig])(implicit log: Logger) = {
    val emr = buildEmr(settings)
    val clustersNames = emr
      .listClusters(new ListClustersRequest().withClusterStates(activatedClusterStates: _*))
      .getClusters().asScala
      .map(_.getName)
    if (clustersNames.exists(_ == settings.clusterName)) {
      log.error(s"A cluster with name ${settings.clusterName} already exists.")
    } else {
      val request = Some(new RunJobFlowRequest())
        .map(r => settings.s3LoggingFolder.map(folder => r.withLogUri(folder)).getOrElse(r))
        .map(r => stepConfig.map(c => r.withSteps(c)).getOrElse(r))
        .get
        .withName(settings.clusterName)
        .withApplications(new Application().withName("Spark"))
        .withReleaseLabel(settings.emrRelease)
        .withServiceRole(settings.emrServiceRole)
        .withJobFlowRole(settings.instanceRole)
        .withInstances(
          Some(new JobFlowInstancesConfig())
            .map(c => settings.subnetId.map(id => c.withEc2SubnetId(id)).getOrElse(c))
            .map { c =>
              settings.additionalSecurityGroupIds.map { ids =>
                c.withAdditionalMasterSecurityGroups(ids: _*).withAdditionalSlaveSecurityGroups(ids: _*)
              }.getOrElse(c)
            }
            .get
            .withInstanceGroups({
              val masterConfig = Some(new InstanceGroupConfig())
                .map { c =>
                  settings.instanceBidPrice.map { price =>
                    c.withMarket("SPOT").withBidPrice(price)
                  }.getOrElse(c)
                }
                .get
                .withInstanceCount(1)
                .withInstanceRole("MASTER")
                .withInstanceType(settings.instanceType)

              val slaveCount = settings.instanceCount - 1
              val slaveConfig = Some(new InstanceGroupConfig())
                .map { c =>
                  settings.instanceBidPrice.map { price =>
                    c.withMarket("SPOT").withBidPrice(price)
                  }.getOrElse(c)
                }
                .get
                .withInstanceCount(slaveCount)
                .withInstanceRole("CORE")
                .withInstanceType(settings.instanceType)

              if (slaveCount <= 0) {
                Seq(masterConfig).asJava
              } else {
                Seq(masterConfig, slaveConfig).asJava
              }
            })
            .withKeepJobFlowAliveWhenNoSteps(stepConfig.isEmpty)
        )
      val res = emr.runJobFlow(request)
      log.info(s"Your new cluster's id is ${res.getJobFlowId}, you may check its status on AWS console.")
    }
  }

  def submitJob(
    settings: Settings,
    mainClass: String,
    args: Seq[String],
    jar: File
  )(implicit log: Logger) = {
    //validation
    //TODO: avoid throwing exceptions here
    assert(settings.s3JarFolder.startsWith("s3://"), "sparkS3JarLocation should starts with \"s3://\".")
    val pathWithoutPrefix = settings.s3JarFolder.drop(5)

    val bucket = pathWithoutPrefix.split("/").head
    assert(bucket != "", "The bucket name in sparkS3JarLocation is empty.")

    assert(pathWithoutPrefix.endsWith("/"), "sparkS3JarLocation should ends with \"/\".")

    //put jar to s3
    log.info("Uploading the jar.")
    val s3 = AmazonS3ClientBuilder.defaultClient()
    val key = (pathWithoutPrefix.split("/").tail :+ jar.getName).mkString("/")

    s3.putObject(bucket, key, jar)
    log.info("Jar uploaded.")

    val s3JarLocation = "s3://" + bucket + "/" + key

    //find the cluster
    val emr = buildEmr(settings)

    val clusterIdOpt = emr
      .listClusters(new ListClustersRequest().withClusterStates(activatedClusterStates: _*))
      .getClusters().asScala
      .find(_.getName == settings.clusterName)
      .map(_.getId)

    //submit job
    val stepConfig = new StepConfig()
      .withActionOnFailure(ActionOnFailure.CONTINUE)
      .withName("Spark Step")
      .withHadoopJarStep(
        new HadoopJarStepConfig()
          .withJar("command-runner.jar")
          .withArgs((Seq("spark-submit", "--deploy-mode", "cluster", "--class", mainClass, s3JarLocation) ++ args).asJava)
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
        createCluster(settings, Some(stepConfig))
    }
  }

  def buildEmr(settings: Settings): AmazonElasticMapReduce = {
    val builder = AmazonElasticMapReduceClientBuilder.standard()
    builder.setRegion(settings.awsRegion)
    builder.build()
  }
}

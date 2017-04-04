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
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.elasticmapreduce.{AmazonElasticMapReduce, AmazonElasticMapReduceClientBuilder}
import com.amazonaws.services.elasticmapreduce.model.{Unit => _, Configuration => EMRConfiguration, _}
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
    val sparkAdditionalSecurityGroupIdsMaster = settingKey[Seq[String]]("additional security group ids for the master ec2 instance")
    val sparkAdditionalSecurityGroupIdsSlave = settingKey[Seq[String]]("additional security group ids for the slave ec2 instances")
    val sparkS3JarFolder = settingKey[String]("S3 folder for putting the executable jar")
    val sparkS3LoggingFolder = settingKey[Option[String]]("S3 folder for application's logs")
    val sparkClusterConfigurationS3Location = settingKey[Option[String]]("S3 location for the EMR cluster configuration")
    val sparkClusterAdditionalApplications = settingKey[Seq[String]]("Applications other than Spark to be deployed on the EMR cluster, these are case insensitive.")
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
    additionalSecurityGroupIdsMaster: Seq[String],
    additionalSecurityGroupIdsSlave: Seq[String],
    s3JarFolder: String,
    s3LoggingFolder: Option[String],
    clusterConfigurationS3Location: Option[String],
    clusterAdditionalApplications: Seq[String]
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
    sparkAdditionalSecurityGroupIdsMaster := Nil,
    sparkAdditionalSecurityGroupIdsSlave := Nil,
    sparkS3LoggingFolder := None,
    sparkClusterConfigurationS3Location := None,
    sparkClusterAdditionalApplications := Nil,

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
      sparkAdditionalSecurityGroupIdsMaster.value,
      sparkAdditionalSecurityGroupIdsSlave.value,
      sparkS3JarFolder.value,
      sparkS3LoggingFolder.value,
      sparkClusterConfigurationS3Location.value,
      sparkClusterAdditionalApplications.value
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


      val request = new RunJobFlowRequest()

      settings.s3LoggingFolder.foreach(request.setLogUri)

      settings.clusterConfigurationS3Location.foreach { location =>
        request.setConfigurations(loadConfigFromS3(location).asJava)
      }

      stepConfig.foreach(s => request.withSteps(s))

      val applications = ("Spark" +: settings.clusterAdditionalApplications).map(name => new Application().withName(name))

      request.withName(settings.clusterName)
        .withApplications(applications.asJava)
        .withReleaseLabel(settings.emrRelease)
        .withServiceRole(settings.emrServiceRole)
        .withJobFlowRole(settings.instanceRole)
        .withInstances {
          val jobFlowConfig = new JobFlowInstancesConfig()
          settings.subnetId.foreach(jobFlowConfig.setEc2SubnetId)

          if(settings.additionalSecurityGroupIdsMaster.nonEmpty)
            jobFlowConfig.setAdditionalMasterSecurityGroups(settings.additionalSecurityGroupIdsMaster.asJava)

          if(settings.additionalSecurityGroupIdsSlave.nonEmpty)
            jobFlowConfig.setAdditionalSlaveSecurityGroups(settings.additionalSecurityGroupIdsSlave.asJava)

          def newInstanceGroup(): InstanceGroupConfig = {
            val group =  new InstanceGroupConfig()
            settings.instanceBidPrice.fold(
              group.withMarket(MarketType.ON_DEMAND)
            )(
              group.withMarket(MarketType.SPOT).withBidPrice
            )

          }

          jobFlowConfig.withInstanceGroups({
            val masterConfig = newInstanceGroup()

            masterConfig.withInstanceCount(1)
              .withInstanceRole(InstanceRoleType.MASTER)
              .withInstanceType(settings.instanceType)

            val slaveCount = settings.instanceCount - 1

            if (slaveCount <= 0)
              Seq(masterConfig).asJava
            else {
              val slaveConfig = newInstanceGroup()
              slaveConfig.withInstanceCount(slaveCount)
                .withInstanceRole(InstanceRoleType.CORE)
                .withInstanceType(settings.instanceType)

              Seq(masterConfig, slaveConfig).asJava
            }
          }).withKeepJobFlowAliveWhenNoSteps(stepConfig.isEmpty)
        }
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

    log.info("Uploading the jar.")

    val s3JarFolderUrl = S3Url(settings.s3JarFolder)
    val s3 = AmazonS3ClientBuilder.defaultClient()
    val key = s3JarFolderUrl.key.fold(jar.getName)(_ + "/" + jar.getName)

    s3.putObject(s3JarFolderUrl.bucket, key, jar)
    log.info("Jar uploaded.")


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
          .withArgs((Seq("spark-submit", "--deploy-mode", "cluster", "--class", mainClass, s3JarFolderUrl.copy(key = Some(key)).toString) ++ args).asJava)
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

  import play.api.libs.json.Json
  implicit val reads = Json.reads[ConfigScala]

  def loadConfigFromS3(configLocation: String)(implicit log: Logger): List[EMRConfiguration] = {
    val s3 = AmazonS3ClientBuilder.defaultClient()
    val url = S3Url(configLocation)
    if(url.key.isEmpty) {
      sys.error("s3 url for config file is missing a key")
    } else {
    val input = s3.getObject(url.bucket, url.key.get).getObjectContent

    log.info(s"Importing configuration from $configLocation")

    Json.parse(input).validate[List[ConfigScala]].fold(
      _ => sys.error("failed to parse json") ,
      _.map { cs =>
        new EMRConfiguration().
          withClassification(cs.Classification).
          withProperties(cs.Properties.asJava)
      })
    }
  }

  case class ConfigScala(Classification: String, Properties: Map[String, String])

  final case class S3Url(bucket: String, key: Option[String]) {
    override val toString =  s"s3://$bucket/${key.getOrElse("")}"
  }

  object S3Url {
    def apply(url: String): S3Url = {
      if(url.startsWith("s3://")) {
        val (bucket, key) = url.drop(5).split("/").toList match {
          case head :: Nil =>  (head, None)
          case head :: tail => (head, Some(tail.mkString("/")))
          case _ => sys.error(s"unrecognized s3 url: $url")
        }
        S3Url(bucket, key)
      } else sys.error("S3Location should starts with \"s3://\".")
    }
  }

}

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
import com.amazonaws.services.elasticmapreduce.model.{Unit => _, Configuration => AwsEmrConfig, _}
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import sbinary.DefaultProtocol.StringFormat
import sbt._
import sbt.Cache.seqFormat
import sbt.Defaults.runMainParser
import sbt.Keys._
import sbt.complete.DefaultParsers._
import sbtassembly.AssemblyKeys._
import sbtassembly.AssemblyPlugin

case class EmrConfig(
  classification: String,
  properties: Map[String, String] = Map.empty,
  emrConfigs: Seq[EmrConfig] = Seq.empty
) {
  def toAwsEmrConfig(): AwsEmrConfig = {
    new AwsEmrConfig()
      .withClassification(classification)
      .withProperties(properties.asJava)
      .withConfigurations(emrConfigs.map(_.toAwsEmrConfig()): _*)
  }
}

object EmrSparkPlugin extends AutoPlugin {
  object autoImport {
    //configs
    val sparkClusterName = settingKey[String]("emr cluster's name")
    val sparkAwsRegion = settingKey[String]("aws's region")
    val sparkEmrRelease = settingKey[String]("emr's release label")
    val sparkEmrServiceRole = settingKey[String]("emr's service role")
    val sparkEmrConfigs = settingKey[Option[Seq[EmrConfig]]]("EMR configurations")
    val sparkSubnetId = settingKey[Option[String]]("spark's subnet id")
    val sparkSecurityGroupIds = settingKey[Option[Seq[String]]]("additional security group ids for both master and slave ec2 instances")
    val sparkInstanceCount = settingKey[Int]("total number of instances")
    val sparkInstanceType = settingKey[String]("spark nodes' instance type")
    val sparkInstanceBidPrice = settingKey[Option[String]]("spark nodes' bid price")
    val sparkInstanceRole = settingKey[String]("spark ec2 instance's role")
    val sparkS3JarFolder = settingKey[String]("S3 folder for putting the executable jar")

    //underlying configs
    val sparkEmrClientBuilder = settingKey[AmazonElasticMapReduceClientBuilder]("default EMR client builder")
    val sparkS3ClientBuilder = settingKey[AmazonS3ClientBuilder]("default S3 client builder")
    val sparkJobFlowInstancesConfig = settingKey[JobFlowInstancesConfig]("default JobFlowInstancesConfig")
    val sparkRunJobFlowRequest = settingKey[RunJobFlowRequest]("default RunJobFlowRequest")

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

  override lazy val projectSettings = baseSettings

  lazy val baseSettings = Seq(
    sparkClusterName := name.value,
    sparkAwsRegion := "us-west-2",
    sparkEmrRelease := "emr-5.5.0",
    sparkEmrServiceRole := "EMR_DefaultRole",
    sparkEmrConfigs := None,
    sparkSubnetId := None,
    sparkSecurityGroupIds := None,
    sparkInstanceCount := 1,
    sparkInstanceType := "m3.xlarge",
    sparkInstanceBidPrice := None,
    sparkInstanceRole := "EMR_EC2_DefaultRole",
    sparkS3JarFolder := "changeme",

    sparkEmrClientBuilder := {
      AmazonElasticMapReduceClientBuilder.standard.withRegion(sparkAwsRegion.value)
    },

    sparkS3ClientBuilder := {
      AmazonS3ClientBuilder.standard.withRegion(sparkAwsRegion.value)
    },

    sparkJobFlowInstancesConfig := {
      Some(new JobFlowInstancesConfig())
        .map(c => sparkSubnetId.value.fold(c)(id => c.withEc2SubnetId(id)))
        .map(c => sparkSecurityGroupIds.value.fold(c) { ids =>
          c.withAdditionalMasterSecurityGroups(ids: _*).withAdditionalSlaveSecurityGroups(ids: _*)
        })
        .get
        .withInstanceGroups {
          val masterConfig = Some(new InstanceGroupConfig())
            .map(c => sparkInstanceBidPrice.value.fold(c.withMarket(MarketType.ON_DEMAND))(c.withMarket(MarketType.SPOT).withBidPrice))
            .get
            .withInstanceCount(1)
            .withInstanceRole(InstanceRoleType.MASTER)
            .withInstanceType(sparkInstanceType.value)

          val slaveCount = sparkInstanceCount.value - 1
          val slaveConfig = Some(new InstanceGroupConfig())
            .map(c => sparkInstanceBidPrice.value.fold(c.withMarket(MarketType.ON_DEMAND))(c.withMarket(MarketType.SPOT).withBidPrice))
            .get
            .withInstanceCount(slaveCount)
            .withInstanceRole(InstanceRoleType.CORE)
            .withInstanceType(sparkInstanceType.value)

          if (slaveCount <= 0) {
            Seq(masterConfig).asJava
          } else {
            Seq(masterConfig, slaveConfig).asJava
          }
        }
        .withKeepJobFlowAliveWhenNoSteps(true)
    },

    sparkRunJobFlowRequest := {
      Some(new RunJobFlowRequest())
        .map(r => sparkEmrConfigs.value.fold(r) { emrConfigs =>
          r.withConfigurations(emrConfigs.map(_.toAwsEmrConfig()): _*)
        })
        .get
        .withName(sparkClusterName.value)
        .withApplications(new Application().withName("Spark"))
        .withReleaseLabel(sparkEmrRelease.value)
        .withServiceRole(sparkEmrServiceRole.value)
        .withJobFlowRole(sparkInstanceRole.value)
        .withInstances(sparkJobFlowInstancesConfig.value)
    },

    sparkCreateCluster := {
      implicit val log = streams.value.log
      implicit val emr = sparkEmrClientBuilder.value.build()
      findClusterWithName(sparkClusterName.value) match {
        case Some(cluster) =>
          sys.error(s"A cluster with name ${sparkClusterName.value} and id ${cluster.getId} already exists.")
        case None =>
          val res = emr.runJobFlow(sparkRunJobFlowRequest.value)
          log.info(s"Your new cluster's id is ${res.getJobFlowId}, you may check its status on AWS console.")
      }
    },

    sparkSubmitJob := {
      Def.inputTaskDyn {
        implicit val log = streams.value.log
        implicit val emr = sparkEmrClientBuilder.value.build()
        val args = spaceDelimited("<arg>").parsed
        val mainClassValue = (mainClass in Compile).value.getOrElse(sys.error("Can't locate the main class in your application."))
        submitJob(mainClassValue, args)
      }.evaluated
    },

    sparkSubmitJobWithMain := {
      Def.inputTaskDyn {
        implicit val log = streams.value.log
        implicit val emr = sparkEmrClientBuilder.value.build()
        val (mainClass, args) = loadForParser(discoveredMainClasses in Compile)((s, names) => runMainParser(s, names getOrElse Nil)).parsed
        submitJob(mainClass, args)
      }.evaluated
    },

    sparkListClusters := {
      val log = streams.value.log
      val emr = sparkEmrClientBuilder.value.build()

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
    },

    sparkTerminateCluster := {
      val log = streams.value.log
      implicit val emr = sparkEmrClientBuilder.value.build()

      val clusterIdOpt = emr
        .listClusters(new ListClustersRequest().withClusterStates(activatedClusterStates: _*))
        .getClusters().asScala
        .find(_.getName == sparkClusterName.value)
        .map(_.getId)

      findClusterWithName(sparkClusterName.value) match {
        case None =>
          log.info(s"The cluster with name ${sparkClusterName.value} does not exist.")
        case Some(cluster) =>
          emr.terminateJobFlows(new TerminateJobFlowsRequest().withJobFlowIds(cluster.getId))
          log.info(s"Cluster with id ${cluster.getId} is terminating, please check aws console for the following information.")
      }
    }
  )

  def submitJob(mainClass: String, args: Seq[String])(implicit log: Logger, emr: AmazonElasticMapReduce) = Def.task {
    val jar = assembly.value
    val s3Jar = new S3Url(sparkS3JarFolder.value) / jar.getName
    log.info(s"Putting ${jar.getPath} to ${s3Jar.toString}")
    sparkS3ClientBuilder.value.build().putObject(s3Jar.bucket, s3Jar.key, jar)

    val step = new StepConfig()
      .withActionOnFailure(ActionOnFailure.CONTINUE)
      .withName("Spark Step")
      .withHadoopJarStep(
        new HadoopJarStepConfig()
          .withJar("command-runner.jar")
          .withArgs((Seq("spark-submit", "--deploy-mode", "cluster", "--class", mainClass, s3Jar.toString) ++ args).asJava)
      )

    findClusterWithName(sparkClusterName.value) match {
      case Some(cluster) =>
        emr.addJobFlowSteps(new AddJobFlowStepsRequest().withJobFlowId(cluster.getId).withSteps(step))
        log.info(s"Your job is added to the cluster with id ${cluster.getId}, you may check its status on AWS console.")
      case None =>
        val jobFlowRequest = sparkRunJobFlowRequest.value
          .withSteps((sparkRunJobFlowRequest.value.getSteps.asScala :+ step): _*)
          .withInstances(sparkJobFlowInstancesConfig.value.withKeepJobFlowAliveWhenNoSteps(false))
        val res = emr.runJobFlow(jobFlowRequest)
        log.info(s"Your new cluster's id is ${res.getJobFlowId}, you may check its status on AWS console.")
    }
  }

  def findClusterWithName(name: String)(implicit emr: AmazonElasticMapReduce) = {
    emr.listClusters(new ListClustersRequest().withClusterStates(activatedClusterStates: _*))
      .getClusters().asScala
      .find(_.getName == name)
  }

  class S3Url(url: String) {
    require(
      url.startsWith("s3://"),
      "S3 url should start with \"s3://\". Did you forget to set the sparkS3JarFolder key?"
    )

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

/*
 * Copyright 2017 Pishen Tsai
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package sbtemrspark

import com.amazonaws.services.elasticmapreduce.model.{Unit => _, _}
import com.amazonaws.services.elasticmapreduce.{
  AmazonElasticMapReduce,
  AmazonElasticMapReduceClientBuilder
}
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.PutObjectRequest
import sbt.Defaults.runMainParser
import sbt.Keys._
import sbt._
import sbt.complete.DefaultParsers._
import sbtassembly.AssemblyKeys._
import sbtassembly.AssemblyPlugin
import sjsonnew.BasicJsonProtocol._

import scala.collection.JavaConverters._
import scala.concurrent.duration._

object EmrSparkPlugin extends AutoPlugin {
  object autoImport {
    //configs
    val sparkClusterName = settingKey[String]("emr cluster's name")
    val sparkAwsRegion = settingKey[String]("aws's region")
    val sparkEmrRelease = settingKey[String]("emr's release label")
    val sparkEmrServiceRole = settingKey[String]("emr's service role")
    val sparkEmrConfigs =
      settingKey[Option[Seq[EmrConfig]]]("EMR configurations")
    val sparkSubnetId = settingKey[Option[String]]("spark's subnet id")
    val sparkSecurityGroupIds = settingKey[Option[Seq[String]]](
      "additional security group ids for both master and slave ec2 instances")
    val sparkInstanceCount = settingKey[Int]("total number of instances")
    val sparkInstanceType = settingKey[String]("spark nodes' instance type")
    val sparkInstanceBidPrice =
      settingKey[Option[String]]("spark nodes' bid price")
    val sparkInstanceRole = settingKey[String]("spark ec2 instance's role")
    val sparkS3JarFolder =
      settingKey[String]("S3 folder for putting the executable jar")
    val sparkTimeoutDuration =
      settingKey[Duration]("timeout duration for sparkTimeout")

    //underlying configs
    val sparkEmrClientBuilder =
      settingKey[AmazonElasticMapReduceClientBuilder](
        "default EMR client builder")
    val sparkS3ClientBuilder =
      settingKey[AmazonS3ClientBuilder]("default S3 client builder")
    val sparkJobFlowInstancesConfig =
      settingKey[JobFlowInstancesConfig]("default JobFlowInstancesConfig")
    val sparkRunJobFlowRequest =
      settingKey[RunJobFlowRequest]("default RunJobFlowRequest")
    val sparkS3PutObjectDecorator =
      settingKey[PutObjectRequest => PutObjectRequest](
        "Allow user to set metadata with put request.Like server side encryption")
    val sparkSubmitConfs =
      settingKey[Map[String, String]]("Allow user to set spark submit conf")
    val sparkEc2KeyName =
      settingKey[Option[String]]("Allow user to specify sparkEc2KeyName")

    //commands
    val sparkCreateCluster = taskKey[Unit]("create cluster")
    val sparkListClusters = taskKey[Unit]("list existing active clusters")
    val sparkTerminateCluster = taskKey[Unit]("terminate cluster")
    val sparkSubmitJob = inputKey[Unit]("submit the job")
    val sparkSubmitJobWithMain =
      inputKey[Unit]("submit the job with specified main class")
    val sparkMonitor =
      taskKey[Unit]("monitor and terminate the cluster when timeout")
  }
  import autoImport._

  override def trigger = allRequirements
  override def requires = AssemblyPlugin

  val activatedClusterStates = Seq(
    ClusterState.RUNNING,
    ClusterState.STARTING,
    ClusterState.WAITING,
    ClusterState.BOOTSTRAPPING
  )

  override lazy val projectSettings = baseSettings

  lazy val baseSettings = Seq(
    sparkClusterName := name.value,
    sparkAwsRegion := "us-west-2",
    sparkEmrRelease := "emr-5.9.0",
    sparkEmrServiceRole := "EMR_DefaultRole",
    sparkEmrConfigs := None,
    sparkSubnetId := None,
    sparkSecurityGroupIds := None,
    sparkInstanceCount := 1,
    sparkInstanceType := "m3.xlarge",
    sparkInstanceBidPrice := None,
    sparkInstanceRole := "EMR_EC2_DefaultRole",
    sparkS3JarFolder := "changeme",
    sparkTimeoutDuration := 90.minutes,
    sparkS3PutObjectDecorator := { (req: PutObjectRequest) =>
      req
    },
    sparkSubmitConfs := Map.empty[String, String],
    sparkEc2KeyName := None,
    sparkEmrClientBuilder := {
      AmazonElasticMapReduceClientBuilder.standard
        .withRegion(sparkAwsRegion.value)
    },
    sparkS3ClientBuilder := {
      AmazonS3ClientBuilder.standard.withRegion(sparkAwsRegion.value)
    },
    sparkJobFlowInstancesConfig := {
      Some(new JobFlowInstancesConfig())
        .map(c => sparkSubnetId.value.fold(c)(id => c.withEc2SubnetId(id)))
        .map(c => sparkEc2KeyName.value.fold(c)(id => c.withEc2KeyName(id)))
        .map { c =>
          sparkSecurityGroupIds.value.fold(c) { ids =>
            c.withAdditionalMasterSecurityGroups(ids: _*)
              .withAdditionalSlaveSecurityGroups(ids: _*)
          }
        }
        .get
        .withInstanceGroups {
          val masterConfig = Some(new InstanceGroupConfig())
            .map { c =>
              sparkInstanceBidPrice.value.fold {
                c.withMarket(MarketType.ON_DEMAND)
              } {
                c.withMarket(MarketType.SPOT).withBidPrice
              }
            }
            .get
            .withInstanceCount(1)
            .withInstanceRole(InstanceRoleType.MASTER)
            .withInstanceType(sparkInstanceType.value)

          val slaveCount = sparkInstanceCount.value - 1
          val slaveConfig = Some(new InstanceGroupConfig())
            .map { c =>
              sparkInstanceBidPrice.value.fold {
                c.withMarket(MarketType.ON_DEMAND)
              } {
                c.withMarket(MarketType.SPOT).withBidPrice
              }
            }
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
        .map { r =>
          sparkEmrConfigs.value.fold(r) { emrConfigs =>
            r.withConfigurations(emrConfigs.map(_.toAwsEmrConfig()): _*)
          }
        }
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
          sys.error(
            s"A cluster with name ${sparkClusterName.value} and id ${cluster.getId} already exists.")
        case None =>
          val res = emr.runJobFlow(sparkRunJobFlowRequest.value)
          log.info(
            s"Your new cluster's id is ${res.getJobFlowId}, you may check its status on AWS console.")
      }
    },
    sparkSubmitJob := {
      Def.inputTaskDyn {
        implicit val log = streams.value.log
        implicit val emr = sparkEmrClientBuilder.value.build()
        val args = spaceDelimited("<arg>").parsed
        val mainClassValue = (mainClass in Compile).value.getOrElse(
          sys.error("Can't locate the main class in your application."))
        submitJob(mainClassValue, args, sparkSubmitConfs.value)
      }.evaluated
    },
    sparkSubmitJobWithMain := {
      Def.inputTaskDyn {
        implicit val log = streams.value.log
        implicit val emr = sparkEmrClientBuilder.value.build()
        val (mainClass, args) =
          loadForParser(discoveredMainClasses in Compile) { (s, names) =>
            runMainParser(s, names getOrElse Nil)
          }.parsed
        submitJob(mainClass, args, sparkSubmitConfs.value)
      }.evaluated
    },
    sparkListClusters := {
      val log = streams.value.log
      val emr = sparkEmrClientBuilder.value.build()

      val clusters = emr
        .listClusters(new ListClustersRequest()
          .withClusterStates(activatedClusterStates: _*))
        .getClusters()
        .asScala

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
        .listClusters(new ListClustersRequest()
          .withClusterStates(activatedClusterStates: _*))
        .getClusters()
        .asScala
        .find(_.getName == sparkClusterName.value)
        .map(_.getId)

      findClusterWithName(sparkClusterName.value) match {
        case None =>
          log.info(
            s"The cluster with name ${sparkClusterName.value} does not exist.")
        case Some(cluster) =>
          emr.terminateJobFlows {
            new TerminateJobFlowsRequest().withJobFlowIds(cluster.getId)
          }
          log.info(
            s"Cluster with id ${cluster.getId} is terminating, please check aws console for the following information.")
      }
    },
    sparkMonitor := {
      val log = streams.value.log
      implicit val emr = sparkEmrClientBuilder.value.build()

      findClusterWithName(sparkClusterName.value) match {
        case None =>
          log.info(
            s"The cluster with name ${sparkClusterName.value} does not exist.")
        case Some(cluster) =>
          log.info(s"Found cluster ${cluster.getId}, start monitoring.")
          val timeoutTime = System.currentTimeMillis() +
            sparkTimeoutDuration.value.toMillis
          def checkStatus(): Unit = {
            print(".")
            val updatedCluster = emr.describeCluster {
              new DescribeClusterRequest().withClusterId(cluster.getId)
            }.getCluster
            val state = updatedCluster.getStatus.getState
            val timeout = System.currentTimeMillis() >= timeoutTime
            val activated =
              activatedClusterStates.map(_.toString).contains(state)
            if (timeout && activated) {
              emr.terminateJobFlows {
                new TerminateJobFlowsRequest().withJobFlowIds(cluster.getId)
              }
              println()
              sys.error("Timeout. Cluster terminated.")
            } else if (!activated) {
              val hasAbnormalStep = emr
                .listSteps(new ListStepsRequest().withClusterId(cluster.getId))
                .getSteps
                .asScala
                .map(_.getStatus.getState)
                .exists(_ != StepState.COMPLETED.toString)
              if (hasAbnormalStep) {
                println()
                sys.error("Cluster terminated with abnormal step.")
              } else {
                println()
                log.info("Cluster terminated without error.")
              }
            } else {
              Thread.sleep(5000)
              checkStatus()
            }
          }
          checkStatus()
      }
    }
  )

  def submitJob(
      mainClass: String,
      args: Seq[String],
      sparkConfs: Map[String, String]
  )(implicit log: Logger, emr: AmazonElasticMapReduce) = Def.task {
    val jar = assembly.value
    val s3Jar = new S3Url(sparkS3JarFolder.value) / jar.getName
    log.info(s"Putting ${jar.getPath} to ${s3Jar.toString}")

    val putRequest = sparkS3PutObjectDecorator.value(
      new PutObjectRequest(s3Jar.bucket, s3Jar.key, jar)
    )
    sparkS3ClientBuilder.value.build().putObject(putRequest)

    val sparkSubmitArgs = Seq(
      "spark-submit",
      "--deploy-mode",
      "cluster",
      "--class",
      mainClass
    ) ++ sparkConfs.toSeq.flatMap {
      case (k, v) => Seq("--conf", k + "=" + v)
    } ++ (s3Jar.toString +: args)

    val step = new StepConfig()
      .withActionOnFailure(ActionOnFailure.CONTINUE)
      .withName("Spark Step")
      .withHadoopJarStep(
        new HadoopJarStepConfig()
          .withJar("command-runner.jar")
          .withArgs(sparkSubmitArgs.asJava)
      )

    findClusterWithName(sparkClusterName.value) match {
      case Some(cluster) =>
        emr.addJobFlowSteps(
          new AddJobFlowStepsRequest()
            .withJobFlowId(cluster.getId)
            .withSteps(step))
        log.info(
          s"Your job is added to the cluster with id ${cluster.getId}, you may check its status on AWS console.")
      case None =>
        val jobFlowRequest = sparkRunJobFlowRequest.value
          .withSteps(
            (sparkRunJobFlowRequest.value.getSteps.asScala :+ step): _*)
          .withInstances(sparkJobFlowInstancesConfig.value
            .withKeepJobFlowAliveWhenNoSteps(false))
        val res = emr.runJobFlow(jobFlowRequest)
        log.info(
          s"Your new cluster's id is ${res.getJobFlowId}, you may check its status on AWS console.")
    }
  }

  def findClusterWithName(name: String)(implicit emr: AmazonElasticMapReduce) = {
    emr
      .listClusters {
        new ListClustersRequest().withClusterStates(activatedClusterStates: _*)
      }
      .getClusters()
      .asScala
      .find(_.getName == name)
  }
}

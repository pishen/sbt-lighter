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

package sbtlighter

import com.amazonaws.services.elasticmapreduce.model.{Unit => _, _}
import com.amazonaws.services.elasticmapreduce._
import com.amazonaws.services.s3._
import com.amazonaws.services.s3.model.PutObjectRequest
import sbt._
import sbt.Defaults.runMainParser
import sbt.Keys._
import sbt.complete.DefaultParsers._
import sbt.io.IO
import sbtassembly.AssemblyKeys._
import sbtassembly.AssemblyPlugin
import sjsonnew.BasicJsonProtocol._

import scala.collection.JavaConverters._
import scala.concurrent.duration._

object LighterPlugin extends AutoPlugin {
  object autoImport {
    //configs
    val sparkClusterName =
      settingKey[String]("EMR Cluster Name")
    val sparkClusterIdFile =
      settingKey[File]("EMR Cluster Id File")
    val sparkAwsRegion =
      settingKey[String]("AWS Region")
    val sparkEmrRelease =
      settingKey[String]("EMR Release Label")
    val sparkEmrServiceRole =
      settingKey[String]("EMR Service Role")
    val sparkEmrConfigs =
      settingKey[Seq[EmrConfig]]("EMR Configurations")
    val sparkEmrApplications =
      settingKey[Seq[String]]("EMR Applications")
    val sparkVisibleToAllUsers =
      settingKey[Boolean]("EMR Cluster Visiblity")
    val sparkSubnetId =
      settingKey[Option[String]]("Subnet Id")
    val sparkSecurityGroupIds =
      settingKey[Seq[String]](
        "Additional security groups for the EC2 instances"
      )
    val sparkInstanceCount =
      settingKey[Int]("Total Number of Instances")
    val sparkInstanceRole =
      settingKey[String]("EC2 Instance Role")
    val sparkInstanceKeyName =
      settingKey[Option[String]]("EC2 Keypair Name")
    val sparkMasterType =
      settingKey[String]("Instance Type of EMR Master")
    val sparkMasterPrice =
      settingKey[Option[Double]]("Spot Price of EMR Master")
    val sparkCoreType =
      settingKey[String]("Instance Type of EMR Core")
    val sparkCorePrice =
      settingKey[Option[Double]]("Spot Price of EMR Core")
    val sparkS3JarFolder =
      settingKey[String]("S3 folder for the built jar")
    val sparkS3LogUri =
      settingKey[Option[String]]("S3 folder for EMR logs")
    val sparkTimeoutDuration =
      settingKey[Duration]("Timeout duration for sparkMonitor")
    val sparkSubmitConfs =
      settingKey[Map[String, String]](
        "The configs of --conf when running spark-submit"
      )

    //underlying configs
    val sparkEmrClient =
      settingKey[AmazonElasticMapReduce]("Default EMR Client")
    val sparkS3Client =
      settingKey[AmazonS3]("Default S3 Client")
    val sparkS3PutObjectDecorator =
      settingKey[PutObjectRequest => PutObjectRequest](
        "Decorator function that will be applied on PutObjectRequest before it is sent"
      )
    val sparkJobFlowInstancesConfig =
      settingKey[JobFlowInstancesConfig]("Default JobFlowInstancesConfig")
    val sparkRunJobFlowRequest =
      settingKey[RunJobFlowRequest]("Default RunJobFlowRequest")

    //commands
    val sparkClusterId =
      taskKey[Option[String]]("Cluster Id in sparkClusterIdFile")
    val sparkCreateCluster =
      taskKey[Unit]("Create cluster")
    val sparkListClusters =
      taskKey[Unit]("List active clusters")
    val sparkBindCluster =
      inputKey[Unit]("Bind to a cluster")
    val sparkTerminateCluster =
      taskKey[Unit]("Terminate cluster")
    val sparkSubmit =
      inputKey[Unit]("Submit the job")
    val sparkSubmitMain =
      inputKey[Unit]("Submit the job with specified main class")
    val sparkMonitor =
      taskKey[Unit]("Monitor and terminate the cluster when timeout")
  }
  import autoImport._

  override def trigger = allRequirements
  override def requires = AssemblyPlugin

  override lazy val projectSettings = baseSettings

  lazy val disable = Seq(
    sparkAwsRegion := "disable",
    sparkCreateCluster := {},
    sparkListClusters := {},
    sparkBindCluster := {},
    sparkTerminateCluster := {},
    sparkSubmit := {},
    sparkSubmitMain := {},
    sparkMonitor := {}
  )

  lazy val baseSettings = Seq(
    sparkClusterName := name.value,
    sparkClusterIdFile := file(".cluster_id"),
    sparkEmrRelease := "emr-5.11.0",
    sparkEmrServiceRole := "EMR_DefaultRole",
    sparkEmrConfigs := Seq.empty,
    sparkEmrApplications := Seq("Spark"),
    sparkVisibleToAllUsers := true,
    sparkSubnetId := None,
    sparkSecurityGroupIds := Seq.empty,
    sparkInstanceCount := 1,
    sparkInstanceRole := "EMR_EC2_DefaultRole",
    sparkInstanceKeyName := None,
    sparkMasterType := "m3.xlarge",
    sparkMasterPrice := None,
    sparkCoreType := "m3.xlarge",
    sparkCorePrice := None,
    sparkS3LogUri := None,
    sparkTimeoutDuration := 90.minutes,
    sparkSubmitConfs := Map.empty,
    sparkEmrClient := {
      AmazonElasticMapReduceClientBuilder.standard
        .withRegion(sparkAwsRegion.value)
        .build
    },
    sparkS3Client := {
      AmazonS3ClientBuilder.standard
        .withRegion(sparkAwsRegion.value)
        .build
    },
    sparkS3PutObjectDecorator := identity,
    sparkJobFlowInstancesConfig := {
      Some(new JobFlowInstancesConfig())
        .map { c =>
          sparkSubnetId.value
            .map(id => c.withEc2SubnetId(id))
            .getOrElse(c)
        }
        .map { c =>
          sparkInstanceKeyName.value
            .map(id => c.withEc2KeyName(id))
            .getOrElse(c)
        }
        .map { c =>
          val ids = sparkSecurityGroupIds.value
          if (ids.nonEmpty) {
            c.withAdditionalMasterSecurityGroups(ids: _*)
              .withAdditionalSlaveSecurityGroups(ids: _*)
          } else c
        }
        .get
        .withInstanceGroups {
          val masterConfig = Some(new InstanceGroupConfig())
            .map { c =>
              sparkMasterPrice.value
                .map { price =>
                  c.withMarket(MarketType.SPOT).withBidPrice(price.toString)
                }
                .getOrElse(c.withMarket(MarketType.ON_DEMAND))
            }
            .get
            .withInstanceCount(1)
            .withInstanceRole(InstanceRoleType.MASTER)
            .withInstanceType(sparkMasterType.value)

          val coreCount = sparkInstanceCount.value - 1
          val coreConfig = Some(new InstanceGroupConfig())
            .map { c =>
              sparkCorePrice.value
                .map { price =>
                  c.withMarket(MarketType.SPOT).withBidPrice(price.toString)
                }
                .getOrElse(c.withMarket(MarketType.ON_DEMAND))
            }
            .get
            .withInstanceCount(coreCount)
            .withInstanceRole(InstanceRoleType.CORE)
            .withInstanceType(sparkCoreType.value)

          if (coreCount <= 0) {
            Seq(masterConfig).asJava
          } else {
            Seq(masterConfig, coreConfig).asJava
          }
        }
        .withKeepJobFlowAliveWhenNoSteps(true)
    },
    sparkRunJobFlowRequest := {
      Some(new RunJobFlowRequest())
        .map { r =>
          val emrConfigs = sparkEmrConfigs.value
          if (emrConfigs.nonEmpty) {
            r.withConfigurations(emrConfigs.map(_.toAwsEmrConfig()): _*)
          } else r
        }
        .map { r =>
          sparkS3LogUri.value.map(r.withLogUri).getOrElse(r)
        }
        .get
        .withName(sparkClusterName.value)
        .withApplications(
          sparkEmrApplications.value.map(a => new Application().withName(a)): _*
        )
        .withReleaseLabel(sparkEmrRelease.value)
        .withServiceRole(sparkEmrServiceRole.value)
        .withJobFlowRole(sparkInstanceRole.value)
        .withVisibleToAllUsers(sparkVisibleToAllUsers.value)
        .withInstances(sparkJobFlowInstancesConfig.value)
    },
    sparkClusterId := {
      Some(sparkClusterIdFile.value)
        .filter(_.exists)
        .map(f => IO.read(f))
    },
    sparkCreateCluster := {
      val logger = streams.value.log
      implicit val emr = sparkEmrClient.value
      val clusterOpt = sparkClusterId.value.flatMap { id =>
        getClusters().find(_.getId == id)
      }
      clusterOpt match {
        case Some(cluster) =>
          sys.error(
            s"Cluster ${cluster.getId} is still active. Please check your ${sparkClusterIdFile.value}."
          )
        case None =>
          val res = emr.runJobFlow(sparkRunJobFlowRequest.value)
          IO.write(sparkClusterIdFile.value, res.getJobFlowId.getBytes)
          logger.info(s"Cluster ${res.getJobFlowId} is created.")
      }
    },
    sparkListClusters := {
      val logger = streams.value.log
      implicit val emr = sparkEmrClient.value
      val clusters = getClusters()
      if (clusters.isEmpty) {
        logger.info("No active cluster found.")
      } else {
        logger.info(s"${clusters.size} active clusters found: ")
        clusters.toSeq.sortBy(_.getName).foreach { c =>
          logger.info(s"Id: ${c.getId} | Name: ${c.getName}")
        }
      }
    },
    sparkBindCluster := {
      val logger = streams.value.log
      implicit val emr = sparkEmrClient.value
      val targetId = spaceDelimited("<arg>").parsed.head
      val clusterOpt = sparkClusterId.value.flatMap { id =>
        getClusters().find(_.getId == id)
      }
      clusterOpt match {
        case Some(cluster) =>
          sys.error(
            s"Cluster ${cluster.getId} is still active. Please check your ${sparkClusterIdFile.value}."
          )
        case None =>
          IO.write(sparkClusterIdFile.value, targetId.getBytes)
          logger.info(s"Bound to cluster ${targetId}.")
      }
    },
    sparkTerminateCluster := {
      val logger = streams.value.log
      implicit val emr = sparkEmrClient.value
      val clusterOpt = sparkClusterId.value.flatMap { id =>
        getClusters().find(_.getId == id)
      }
      clusterOpt match {
        case Some(cluster) =>
          emr.terminateJobFlows(
            new TerminateJobFlowsRequest().withJobFlowIds(cluster.getId)
          )
          IO.delete(sparkClusterIdFile.value)
          logger.info(s"Cluster ${cluster.getId} is terminating.")
        case None =>
          logger.info("Can't find any cluster to terminate.")
      }
    },
    sparkSubmit := {
      implicit val logger = streams.value.log
      implicit val emr = sparkEmrClient.value
      implicit val s3 = sparkS3Client.value
      val args = spaceDelimited("<arg>").parsed
      val mainClassValue = (mainClass in Compile).value.getOrElse(
        sys.error("Can't locate the main class in your application.")
      )
      submitJob(
        mainClassValue,
        args,
        sparkSubmitConfs.value,
        assembly.value,
        S3Url(sparkS3JarFolder.value),
        sparkS3PutObjectDecorator.value,
        sparkClusterIdFile.value,
        sparkClusterId.value,
        sparkRunJobFlowRequest.value
      )
    },
    sparkSubmitMain := {
      implicit val logger = streams.value.log
      implicit val emr = sparkEmrClient.value
      implicit val s3 = sparkS3Client.value
      val (mainClass, args) = loadForParser(
        discoveredMainClasses in Compile
      ) { (s, names) =>
        runMainParser(s, names getOrElse Nil)
      }.parsed

      submitJob(
        mainClass,
        args,
        sparkSubmitConfs.value,
        assembly.value,
        S3Url(sparkS3JarFolder.value),
        sparkS3PutObjectDecorator.value,
        sparkClusterIdFile.value,
        sparkClusterId.value,
        sparkRunJobFlowRequest.value
      )
    },
    sparkMonitor := {
      val logger = streams.value.log
      implicit val emr = sparkEmrClient.value
      val clusterOpt = sparkClusterId.value.flatMap { id =>
        getClusters().find(_.getId == id)
      }
      clusterOpt match {
        case None =>
          logger.info("Can't find the cluster to monitor.")
        case Some(cluster) =>
          logger.info(s"Found cluster ${cluster.getId}, start monitoring.")
          val timeoutTime = System.currentTimeMillis() +
            sparkTimeoutDuration.value.toMillis
          def checkStatus(): Unit = {
            print(".")
            val active = getClusters().exists(_.getId == cluster.getId)
            val timeout = System.currentTimeMillis() >= timeoutTime
            if (timeout && active) {
              emr.terminateJobFlows {
                new TerminateJobFlowsRequest().withJobFlowIds(cluster.getId)
              }
              println()
              sys.error("Timeout. Cluster terminated.")
            } else if (!active) {
              val hasAbnormalStep = emr
                .listSteps(
                  new ListStepsRequest().withClusterId(cluster.getId)
                )
                .getSteps
                .asScala
                .map(_.getStatus.getState)
                .exists(_ != StepState.COMPLETED.toString)
              if (hasAbnormalStep) {
                println()
                sys.error("Cluster terminated with abnormal step.")
              } else {
                println()
                logger.info("Cluster terminated without error.")
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

  def getClusters()(implicit emr: AmazonElasticMapReduce) = {
    emr
      .listClusters(
        new ListClustersRequest().withClusterStates(
          ClusterState.RUNNING,
          ClusterState.STARTING,
          ClusterState.WAITING,
          ClusterState.BOOTSTRAPPING
        )
      )
      .getClusters
      .asScala
  }

  def submitJob(
      mainClass: String,
      args: Seq[String],
      sparkConfs: Map[String, String],
      jar: File,
      jarFolder: S3Url,
      putObjectDecorator: PutObjectRequest => PutObjectRequest,
      clusterIdFile: File,
      clusterId: Option[String],
      runJobFlowRequest: RunJobFlowRequest
  )(
      implicit logger: Logger,
      emr: AmazonElasticMapReduce,
      s3: AmazonS3
  ) = {
    val s3Jar = jarFolder / jar.getName
    logger.info(s"Putting ${jar.getPath} to ${s3Jar.toString}")

    val putRequest = putObjectDecorator(
      new PutObjectRequest(s3Jar.bucket, s3Jar.key, jar)
    )
    s3.putObject(putRequest)

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
      .withName(mainClass.split(".").lastOption.getOrElse("Spark Step"))
      .withHadoopJarStep(
        new HadoopJarStepConfig()
          .withJar("command-runner.jar")
          .withArgs(sparkSubmitArgs.asJava)
      )

    clusterId.flatMap(id => getClusters().find(_.getId == id)) match {
      case Some(cluster) =>
        emr.addJobFlowSteps(
          new AddJobFlowStepsRequest()
            .withJobFlowId(cluster.getId)
            .withSteps(step)
        )
        logger.info(s"Your job is added to cluster ${cluster.getId}.")
      case None =>
        val res = emr.runJobFlow(
          runJobFlowRequest
            .withSteps((runJobFlowRequest.getSteps.asScala :+ step): _*)
            .withInstances(
              runJobFlowRequest
                .getInstances()
                .withKeepJobFlowAliveWhenNoSteps(false)
            )
        )
        IO.write(clusterIdFile, res.getJobFlowId.getBytes)
        logger.info(s"Your job is added to cluster ${res.getJobFlowId}.")
    }
  }
}

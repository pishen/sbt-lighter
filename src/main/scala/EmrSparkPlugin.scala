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
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient
import com.amazonaws.services.elasticmapreduce.model.{Unit => _, _}
import com.amazonaws.services.s3.AmazonS3Client
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
    //commands
    val sparkCreateCluster = taskKey[Unit]("create cluster")
    val sparkTerminateCluster = taskKey[Unit]("terminate cluster")
    val sparkSubmitJob = inputKey[Unit]("submit the job")
    val sparkSubmitJobWithMain = inputKey[Unit]("submit the job with specified main class")
  }
  import autoImport._

  override def trigger = allRequirements
  override def requires = AssemblyPlugin

  val activatedClusterStates = Seq(ClusterState.RUNNING, ClusterState.STARTING, ClusterState.WAITING, ClusterState.BOOTSTRAPPING)

  override lazy val projectSettings = Seq(
    sparkClusterName := name.value,
    sparkEmrRelease := "emr-5.2.1",
    sparkEmrServiceRole := "EMR_DefaultRole",
    sparkSubnetId := None,
    sparkInstanceCount := 1,
    sparkInstanceType := "m3.xlarge",
    sparkInstanceBidPrice := None,
    sparkInstanceRole := "EMR_EC2_DefaultRole",
    sparkAdditionalSecurityGroupIds := None,
    sparkS3LoggingFolder := None,

    sparkCreateCluster := {
      val log = streams.value.log

      val emr = new AmazonElasticMapReduceClient()
        .withRegion[AmazonElasticMapReduceClient](Regions.fromName(sparkAwsRegion.value))
      val clustersNames = emr
        .listClusters(new ListClustersRequest().withClusterStates(activatedClusterStates: _*))
        .getClusters().asScala
        .map(_.getName)
      if (clustersNames.exists(_ == sparkClusterName.value)) {
        log.error(s"A cluster with name ${sparkClusterName.value} already exists.")
      } else {
        val request = Some(new RunJobFlowRequest())
          .map(r => sparkS3LoggingFolder.value.map(folder => r.withLogUri(folder)).getOrElse(r))
          .get
          .withName(sparkClusterName.value)
          .withApplications(new Application().withName("Spark"))
          .withReleaseLabel(sparkEmrRelease.value)
          .withServiceRole(sparkEmrServiceRole.value)
          .withJobFlowRole(sparkInstanceRole.value)
          .withInstances(
            Some(new JobFlowInstancesConfig())
              .map(c => sparkSubnetId.value.map(id => c.withEc2SubnetId(id)).getOrElse(c))
              .map { c =>
                sparkAdditionalSecurityGroupIds.value.map { ids =>
                  c.withAdditionalMasterSecurityGroups(ids: _*).withAdditionalSlaveSecurityGroups(ids: _*)
                }.getOrElse(c)
              }
              .get
              .withInstanceGroups({
                val masterConfig = Some(new InstanceGroupConfig())
                  .map { c =>
                    sparkInstanceBidPrice.value.map { price =>
                      c.withMarket("SPOT").withBidPrice(price)
                    }.getOrElse(c)
                  }
                  .get
                  .withInstanceCount(1)
                  .withInstanceRole("MASTER")
                  .withInstanceType(sparkInstanceType.value)

                val slaveCount = sparkInstanceCount.value - 1
                val slaveConfig = Some(new InstanceGroupConfig())
                  .map { c =>
                    sparkInstanceBidPrice.value.map { price =>
                      c.withMarket("SPOT").withBidPrice(price)
                    }.getOrElse(c)
                  }
                  .get
                  .withInstanceCount(slaveCount)
                  .withInstanceRole("CORE")
                  .withInstanceType(sparkInstanceType.value)

                if (slaveCount <= 0) {
                  Seq(masterConfig).asJava
                } else {
                  Seq(masterConfig, slaveConfig).asJava
                }
              })
              .withKeepJobFlowAliveWhenNoSteps(true)
          )
        val res = emr.runJobFlow(request)
        log.info(s"Your new cluster's id is ${res.getJobFlowId}, you may check its status on AWS console.")
      }
    },

    sparkSubmitJob := {
      implicit val log = streams.value.log
      val args = spaceDelimited("<arg>").parsed
      val mainClassValue = (mainClass in Compile).value.getOrElse(sys.error("Can't locate the main class in your application."))
      submitJob(mainClassValue, args, sparkS3JarFolder.value, sparkAwsRegion.value, sparkClusterName.value, assembly.value)
    },

    sparkSubmitJobWithMain := {
      Def.inputTask {
        implicit val log = streams.value.log
        val (mainClass, args) = loadForParser(discoveredMainClasses in Compile)((s, names) => runMainParser(s, names getOrElse Nil)).parsed
        submitJob(mainClass, args, sparkS3JarFolder.value, sparkAwsRegion.value, sparkClusterName.value, assembly.value)
      }.evaluated
    },

    sparkTerminateCluster := {
      val log = streams.value.log

      val emr = new AmazonElasticMapReduceClient()
        .withRegion[AmazonElasticMapReduceClient](Regions.fromName(sparkAwsRegion.value))
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
    }
  )

  def submitJob(
    mainClass: String,
    args: Seq[String],
    s3JarFolder: String,
    awsRegion: String,
    clusterName: String,
    jar: File
  )(implicit log: Logger) = {
    //validation
    //TODO: avoid throwing exceptions here
    assert(s3JarFolder.startsWith("s3://"), "sparkS3JarLocation should starts with \"s3://\".")
    val pathWithoutPrefix = s3JarFolder.drop(5)

    val bucket = pathWithoutPrefix.split("/").head
    assert(bucket != "", "The bucket name in sparkS3JarLocation is empty.")

    assert(pathWithoutPrefix.endsWith("/"), "sparkS3JarLocation should ends with \"/\".")

    val emr = new AmazonElasticMapReduceClient()
      .withRegion[AmazonElasticMapReduceClient](Regions.fromName(awsRegion))
    val clusterId = emr
      .listClusters(new ListClustersRequest().withClusterStates(activatedClusterStates: _*))
      .getClusters().asScala
      .find(_.getName == clusterName)
      .map(_.getId)
      .getOrElse(sys.error(s"The cluster with name ${clusterName} does not exist, you may use sparkCreateCluster to create one first."))

    //put jar to s3
    val s3 = new AmazonS3Client()
    val key = (pathWithoutPrefix.split("/").tail :+ jar.getName).mkString("/")

    s3.putObject(bucket, key, jar)
    log.info("Jar uploaded.")

    val s3JarLocation = "s3://" + bucket + "/" + key

    //submit job
    val res = emr.addJobFlowSteps(
      new AddJobFlowStepsRequest()
        .withJobFlowId(clusterId)
        .withSteps(
          new StepConfig()
            .withActionOnFailure(ActionOnFailure.CONTINUE)
            .withName("Spark Step")
            .withHadoopJarStep(
              new HadoopJarStepConfig()
                .withJar("command-runner.jar")
                .withArgs((Seq("spark-submit", "--deploy-mode", "cluster", "--class", mainClass, s3JarLocation) ++ args).asJava)
            )
        )
    )
    log.info(s"Job submitted with job id ${res.getStepIds.asScala.mkString(",")}")
  }
}

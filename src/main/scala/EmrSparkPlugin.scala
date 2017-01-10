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

import com.amazonaws.regions.Regions
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient
import com.amazonaws.services.elasticmapreduce.model.{Application, ClusterState, JobFlowInstancesConfig, ListClustersRequest, RunJobFlowRequest}
import sbt._
import sbt.Keys._
import sbtassembly.AssemblyPlugin
import scala.collection.JavaConverters._

object EmrSparkPlugin extends AutoPlugin {
  object autoImport {
    //configs
    val sparkClusterName = settingKey[String]("emr cluster's name")
    val sparkAwsRegion = settingKey[String]("aws's region")
    val sparkEmrRelease = settingKey[String]("emr's release label")
    val sparkEmrServiceRole = settingKey[String]("emr's service role")
    val sparkSubnetId = settingKey[Option[String]]("spark's subnet id")
    val sparkInstanceCount = settingKey[Int]("total number of instances")
    val sparkMasterInstanceType = settingKey[String]("spark master's instance type")
    val sparkSlaveInstanceType = settingKey[String]("spark slave's instance type")
    val sparkInstanceRole = settingKey[String]("spark ec2 instance's role")
    //commands
    val sparkCreateCluster = inputKey[Unit]("create cluster")
    val sparkTerminateCluster = taskKey[Unit]("terminate cluster")
    val sparkSubmitJob = inputKey[Unit]("submit the job")
  }
  import autoImport._

  override def trigger = allRequirements
  override def requires = AssemblyPlugin

  override lazy val projectSettings = Seq(
    //TODO
    sparkClusterName := name.value,
    sparkEmrRelease := "emr-5.2.1",
    sparkEmrServiceRole := "EMR_DefaultRole",
    sparkSubnetId := None,
    sparkInstanceCount := 1,
    sparkMasterInstanceType := "m3.xlarge",
    sparkSlaveInstanceType := "m3.xlarge",
    sparkInstanceRole := "EMR_EC2_DefaultRole",

    sparkCreateCluster := {
      val log = streams.value.log

      val emr = new AmazonElasticMapReduceClient()
        .withRegion[AmazonElasticMapReduceClient](Regions.fromName(sparkAwsRegion.value))
      val clustersNames = emr
        .listClusters(new ListClustersRequest().withClusterStates(ClusterState.RUNNING))
        .getClusters().asScala
        .map(_.getName)
      if (clustersNames.exists(_ == sparkClusterName.value)) {
        log.error(s"A cluster with name ${sparkClusterName.value} already exists.")
      } else {
        val request = new RunJobFlowRequest()
          .withName(sparkClusterName.value)
          .withApplications(new Application().withName("Spark"))
          .withReleaseLabel(sparkEmrRelease.value)
          .withServiceRole(sparkEmrServiceRole.value)
          .withJobFlowRole(sparkInstanceRole.value)
          .withInstances(
            Some(new JobFlowInstancesConfig())
              .map(c => sparkSubnetId.value.map(id => c.withEc2SubnetId(id)).getOrElse(c))
              .get
              .withKeepJobFlowAliveWhenNoSteps(true)
              .withInstanceCount(sparkInstanceCount.value)
              .withMasterInstanceType(sparkMasterInstanceType.value)
              .withSlaveInstanceType(sparkSlaveInstanceType.value)
          )
        val res = emr.runJobFlow(request)
        log.info(s"Your new cluster's id is ${res.getJobFlowId}, you may check its status on AWS console.")
      }
    }
  )
}

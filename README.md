# sbt-emr-spark

[![Build Status](https://travis-ci.org/pishen/sbt-emr-spark.svg?branch=master)](https://travis-ci.org/pishen/sbt-emr-spark)

Run your [Spark on AWS EMR](http://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-launch.html) by sbt

## Getting started

1. Add sbt-emr-spark in `project/plugins.sbt`

  ```
  addSbtPlugin("net.pishen" % "sbt-emr-spark" % "0.8.0")
  ```

2. Prepare your `build.sbt`

  ```scala
  name := "sbt-emr-spark-test"

  scalaVersion := "2.11.11"

  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.1.1" % "provided"
  )

  sparkAwsRegion := "ap-northeast-1"

  //(optional) Set the subnet id if you want to run Spark in VPC.
  sparkSubnetId := Some("subnet-xxxxxxxx")

  //(optional) Additional security groups that will be attached to master and slave's ec2.
  sparkSecurityGroupIds := Some(Seq("sg-xxxxxxxx"))

  //Since we use cluster mode, we need a bucket to store your application's jar.
  sparkS3JarFolder := "s3://my-emr-bucket/my-emr-folder/"

  //(optional) Total number of instances, including master node. The default value is 1.
  sparkInstanceCount := 2
  ```

3. Write your application at `src/main/scala/mypackage/Main.scala`

  ```scala
  package mypackage

  import org.apache.spark._

  object Main {
    def main(args: Array[String]): Unit = {
      //setup spark
      val sc = new SparkContext(new SparkConf())
      //your algorithm
      val n = 10000000
      val count = sc.parallelize(1 to n).map { i =>
        val x = scala.math.random
        val y = scala.math.random
        if (x * x + y * y < 1) 1 else 0
      }.reduce(_ + _)
      println("Pi is roughly " + 4.0 * count / n)
    }
  }
  ```

4. Submit your Spark application

  ```
  > sparkSubmitJob arg0 arg1 ...
  ```

> Note that a cluster with the same name as your project's `name` will be created by default if not exist. And this cluster will terminate itself automatically if there's no further jobs (steps) waiting in the queue. (You can submit multiple jobs (steps) into the queue by executing `sparkSubmitJob` multiple times.)
> If you want a keep-alive cluster, which doesn't terminate itself automatically, execute `sparkCreateCluster` before `sparkSubmitJob` and terminate it by `sparkTerminateCluster`:
> ```
> > sparkCreateCluster
> > sparkSubmitJob arg0 arg1 ...
> ```

## Other available settings

```scala
//Your cluster's name. Default value is copied from your project's `name` setting.
sparkClusterName := "your-new-cluster-name"

sparkEmrRelease := "emr-5.5.0"

sparkEmrServiceRole := "EMR_DefaultRole"

//EC2's instance type. Will be applied to both master and slave nodes.
sparkInstanceType := "m3.xlarge"

//Bid price for your spot instance.
//The default value is None, which means all the instance will be on demand.
sparkInstanceBidPrice := Some("0.38")

sparkInstanceRole := "EMR_EC2_DefaultRole"
```

## Other available commands

```
> sparkListClusters

> sparkTerminateCluster

> sparkSubmitJobWithMain mypackage.Main arg0 arg1 ...
```

## Use EmrConfig to configure the applications

EMR provides a JSON syntax to [configure the applications](http://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-configure-apps.html) on cluster, [including spark](http://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-configure.html). Here we provide a helper class called `EmrConfig`, which lets you setup the configuration in an easier way.

For example, to maximize the memory allocation for each Spark job, one can use the following JSON config:

``` javascript
[
  {
    "Classification": "spark",
    "Properties": {
      "maximizeResourceAllocation": "true"
    }
  }
]
```

Instead of using this JSON config, one can add the following setting in `build.sbt` to achieve the same effect:

``` scala
import sbtemrspark.EmrConfig

sparkEmrConfigs := Some(Seq(EmrConfig("spark", Map("maximizeResourceAllocation" -> "true"))))
```

## Modify the configurations of underlying AWS objects

There are two settings called `sparkJobFlowInstancesConfig` and `sparkRunJobFlowRequest`, which corresponds to [JobFlowInstancesConfig](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticmapreduce/model/JobFlowInstancesConfig.html) and [RunJobFlowRequest](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticmapreduce/model/RunJobFlowRequest.html) in AWS Java SDK. Some default values are already configured in these settings, but you can modify it for your own purpose, for example:

To set the S3 logging folder for EMR cluster:

``` scala
sparkRunJobFlowRequest := sparkRunJobFlowRequest.value.withLogUri("s3://aws-logs-xxxxxxxxxxxx-ap-northeast-1/elasticmapreduce/")
```

To set the EC2 keypair for each instances:

``` scala
sparkJobFlowInstancesConfig := sparkJobFlowInstancesConfig.value.withEc2KeyName("your-keypair")
```

To set the master and slave security groups separately (This requires you leaving `sparkSecurityGroupIds` as `None` in step 2):

``` scala
sparkRunJobFlowRequest := sparkRunJobFlowRequest.value
  .withAdditionalMasterSecurityGroups("sg-xxxxxxxx")
  .withAdditionalSlaveSecurityGroups("sg-yyyyyyyy")
```

To set the EMR auto-scaling role:

``` scala
sparkRunJobFlowRequest := sparkRunJobFlowRequest.value.withAutoScalingRole("EMR_AutoScaling_DefaultRole")
```

To add EMR applications other than Spark:

``` scala
import com.amazonaws.services.elasticmapreduce.model.Application

sparkRunJobFlowRequest := sparkRunJobFlowRequest.value
  .withApplications(Seq("Spark", "Presto", "Flink").map(a => new Application().withName(a)):_*)
```

To set the tags on cluster resources:

``` scala
import com.amazonaws.services.elasticmapreduce.model.Tag

sparkRunJobFlowRequest := sparkRunJobFlowRequest.value.withTags(new Tag("Name", "my-cluster-name"))
```

To add some initial steps at cluster creation:

``` scala
import com.amazonaws.services.elasticmapreduce.model._

sparkRunJobFlowRequest := sparkRunJobFlowRequest.value
  .withSteps(
    new StepConfig()
      .withActionOnFailure(ActionOnFailure.CANCEL_AND_WAIT)
      .withName("Install components")
      .withHadoopJarStep(
        new HadoopJarStepConfig()
          .withJar("s3://path/to/jar")
          .withArgs(Seq("arg1", "arg2").asJava)
      )
  )
```

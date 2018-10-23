# sbt-lighter

[![Build Status](https://travis-ci.org/pishen/sbt-lighter.svg?branch=master)](https://travis-ci.org/pishen/sbt-lighter)

SBT plugin for [Spark on AWS EMR](http://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-launch.html).

## Getting started

1. Add sbt-lighter in `project/plugins.sbt`

  ```
  addSbtPlugin("net.pishen" % "sbt-lighter" % "1.2.0")
  ```

2. Setup sbt version for your project in `project/build.properties` (requires sbt 1.0):

  ```
  sbt.version=1.1.6
  ```

3. Prepare your `build.sbt`

  ```scala
  name := "sbt-lighter-demo"

  scalaVersion := "2.11.12"

  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.3.1" % "provided"
  )

  sparkAwsRegion := "ap-northeast-1"

  //Since we use cluster mode, we need a bucket to store our application's jar.
  sparkS3JarFolder := "s3://my-emr-bucket/my-emr-folder/"

  //(optional) Set the subnet id if you want to run Spark in VPC.
  sparkSubnetId := Some("subnet-********")

  //(optional) Additional security groups that will be attached to Master and Core instances.
  sparkSecurityGroupIds := Seq("sg-********")

  //(optional) Total number of instances, including master node. The default value is 1.
  sparkInstanceCount := 2
  ```

4. Write your application at `src/main/scala/mypackage/Main.scala`

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

5. Submit your Spark application

  ```
  > sparkSubmit arg0 arg1 ...
  ```

> Note that a cluster with the same name as your project's `name` will be created by this command. This cluster will terminate itself automatically if there's no further steps (beyond the one you just submitted) waiting in the queue. (You can submit multiple steps into the queue by running `sparkSubmit` multiple times.)
>
> If you want a keep-alive cluster, run `sparkCreateCluster` before `sparkSubmit`, and remember to terminate it with `sparkTerminateCluster` when you are done:
> ```
> > sparkCreateCluster
> > sparkSubmit arg0 arg1 ...
> == Wait for your job to finish ==
> > sparkTerminateCluster
> ```
>
> The id of the created cluster will be stored in a file named `.cluster_id`. This file will be used to find the cluster when running other commands.

## Other available settings

```scala
//Your cluster's name. Default value is copied from your project's `name` setting.
sparkClusterName := "your-new-cluster-name"

sparkClusterIdFile := file(".cluster_id")

sparkEmrRelease := "emr-5.17.0"

sparkEmrServiceRole := "EMR_DefaultRole"

//EMR applications that will be installed, default value is Seq("Spark")
sparkEmrApplications := Seq("Spark", "Zeppelin")

sparkVisibleToAllUsers := true

//EC2 instance type of EMR Master node, default is m4.large
//Note that this is *not* the master node of Spark
sparkMasterType := "m4.large"

//EC2 instance type of EMR Core nodes, default is m4.large
sparkCoreType := "m4.large"

//EBS (disk) size of EMR Master node, default is 32GB
sparkMasterEbsSize := Some(32)

//EBS (disk) size of EMR Core nodes, default is 32GB
sparkCoreEbsSize := Some(32)

//Spot instance bid price of Master node, default is None.
sparkMasterPrice := Some(0.1)

//Spot instance bid price of Core nodes, default is None.
sparkCorePrice := Some(0.1)

sparkInstanceRole := "EMR_EC2_DefaultRole"

//EC2 keypair, default is None.
sparkInstanceKeyName := Some("your-keypair")

//EMR logging folder, default is None.
sparkS3LogUri := Some("s3://my-emr-bucket/my-emr-log-folder/")

//Configs of --conf when running spark-submit, default is an empty Map.
sparkSubmitConfs := Map("spark.executor.memory" -> "10G", "spark.executor.instances" -> "2")

//List of EMR bootstrap scripts and their parameters, if any, default is Seq.empty.
sparkEmrBootstrap := Seq(BootstrapAction("my-bootstrap", "s3://my-production-bucket/bootstrap.sh", "--full"))
```

## Other available commands

```
> show sparkClusterId

> sparkListClusters

> sparkTerminateCluster

> sparkSubmitMain mypackage.Main arg0 arg1 ...
```

If you accidentally deleted your `.cluster_id` file, you can bind it back using:

```
> sparkBindCluster j-*************
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
import sbtlighter.EmrConfig

sparkEmrConfigs := Seq(
  EmrConfig("spark").withProperties("maximizeResourceAllocation" -> "true")
)
```

For people who already have a JSON config, there's a parsing function `EmrConfig.parseJson(jsonString: String)` which can convert the JSON array into `List[EmrConfig]`. And, if your JSON is located on S3, you can also parse the file on S3 directly (note that this will read the file from S3 right after you execute sbt):

``` scala
import sbtlighter.EmrConfig

sparkEmrConfigs := EmrConfig
  .parseJsonFromS3("s3://your-bucket/your-config.json")(sparkS3Client.value)
  .right
  .get
```

## Modify the configurations of underlying AWS objects

There are two settings called `sparkJobFlowInstancesConfig` and `sparkRunJobFlowRequest`, which corresponds to [JobFlowInstancesConfig](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticmapreduce/model/JobFlowInstancesConfig.html) and [RunJobFlowRequest](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/services/elasticmapreduce/model/RunJobFlowRequest.html) in AWS Java SDK. Some default values are already configured in these settings, but you can modify it for your own purpose, for example:

#### To set the master and slave security groups separately (This requires you leaving `sparkSecurityGroupIds` as `None` in step 2):

``` scala
sparkJobFlowInstancesConfig := sparkJobFlowInstancesConfig.value
  .withAdditionalMasterSecurityGroups("sg-aaaaaaaa")
  .withAdditionalSlaveSecurityGroups("sg-bbbbbbbb")
```

#### To set the EMR auto-scaling role:

``` scala
sparkRunJobFlowRequest := sparkRunJobFlowRequest.value.withAutoScalingRole("EMR_AutoScaling_DefaultRole")
```

#### To set the tags on cluster resources:

``` scala
import com.amazonaws.services.elasticmapreduce.model.Tag

sparkRunJobFlowRequest := sparkRunJobFlowRequest.value.withTags(new Tag("Name", "my-cluster-name"))
```

#### To add some initial steps at cluster creation:

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

#### To add Server Side Encryption to Jar File and Add meta data support:

``` scala
import com.amazonaws.services.s3.model.ObjectMetadata
sparkS3PutObjectDecorator := { req =>
  val metadata = new ObjectMetadata()
  metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION)
  req.withMetadata(metadata)
}
```

## Use SBT's config to provide multiple setting combinations

If you have multiple environments (e.g. different subnet, different AWS region, ...etc) for your Spark project, you can use SBT's config to provide multiple setting combinations:

``` scala
import sbtlighter._

//Since we don't use the global scope now, we can disable it.
LighterPlugin.disable

//And setup your configurations
lazy val Testing = config("testing")
lazy val Production = config("production")

inConfig(Testing)(LighterPlugin.baseSettings ++ Seq(
  sparkAwsRegion := "ap-northeast-1",
  sparkSubnetId := Some("subnet-aaaaaaaa"),
  sparkSecurityGroupIds := Seq("sg-aaaaaaaa"),
  sparkInstanceCount := 1,
  sparkS3JarFolder := "s3://my-testing-bucket/my-emr-folder/"
))

inConfig(Production)(LighterPlugin.baseSettings ++ Seq(
  sparkAwsRegion := "us-west-2",
  sparkSubnetId := Some("subnet-bbbbbbbb"),
  sparkSecurityGroupIds := Seq("sg-bbbbbbbb"),
  sparkInstanceCount := 20,
  sparkS3JarFolder := "s3://my-production-bucket/my-emr-folder/",
  sparkS3LogUri := Some("s3://aws-logs-************-us-west-2/elasticmapreduce/")
  sparkCorePrice := Some(0.39),
  sparkEmrConfigs := Seq(EmrConfig("spark", Map("maximizeResourceAllocation" -> "true")))
))
```

Then, in sbt, activate different config by the `<config>:<task/setting>` syntax:

```
> testing:sparkSubmit

> production:sparkSubmit
```

## Keep SBT monitoring the cluster status until it completes

There's a special command called

```
> sparkMonitor
```

which will poll on the cluster's status until it terminates or exceeds the time limit.

The time limit can be defined by:

```scala
import scala.concurrent.duration._

sparkTimeoutDuration := 90.minutes
```
(the default value of `sparkTimeoutDuration` is 90 minutes)

And this command will fall into one of the three following behaviors:

1. If the cluster ran for a duration longer than `sparkTimeoutDuration`, terminate the cluster and throw an exception.
2. If the cluster terminated within `sparkTimeoutDuration` but had some failed steps, throw an exception.
3. If the cluster terminated without any failed step, return Unit (exit code == 0).

This command would be useful if you want to trigger some notifications. For example, a bash command like this

```
$ sbt 'sparkSubmit arg0 arg1' sparkMonitor
```

will exit with error if the job fail or running too long (Don't enter the sbt console here, just append the task names after `sbt` like above). You can then put this command into a cron job for scheduled computation, and let cron notify yourself when something go wrong.

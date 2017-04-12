# sbt-emr-spark

[![Build Status](https://travis-ci.org/pishen/sbt-emr-spark.svg?branch=master)](https://travis-ci.org/pishen/sbt-emr-spark)

Run your [Spark on AWS EMR](http://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-launch.html) by sbt

## Getting started

1. Add sbt-emr-spark in `project/plugins.sbt`

  ```
  addSbtPlugin("net.pishen" % "sbt-emr-spark" % "0.6.0")
  ```

2. Prepare your `build.sbt`

  ```scala
  name := "sbt-emr-spark-test"

  scalaVersion := "2.11.8"

  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.1.0" % "provided"
  )

  sparkAwsRegion := "ap-northeast-1"

  //(optional) Set the subnet id if you want to run Spark in VPC.
  sparkSubnetId := Some("subnet-xxxxxxxx")

  //(optional) Additional security groups that will be attached to master and slave's ec2.
  sparkAdditionalMasterSecurityGroups := Some(Seq("sg-xxxxxxxx"))
  sparkAdditionalSlaveSecurityGroups := sparkAdditionalMasterSecurityGroups.value

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

> Note that a cluster with the same name as your project's `name` will be created by default if not exist. And this cluster will terminate itself automatically if there's no further jobs (steps) waiting in the queue.
> If you want a keep-alive cluster, execute the following command before you submit your first job:
> ```
> > sparkCreateCluster
> ```

## Other available settings

```scala
//Your cluster's name. Default value is copied from your project's `name` setting.
sparkClusterName := "your-new-cluster-name"

sparkEmrRelease := "emr-5.4.0"

sparkEmrServiceRole := "EMR_DefaultRole"

//EC2's instance type. Will applied to both master and slave nodes.
sparkInstanceType := "m3.xlarge"

//Bid price for your spot instance.
//The default value is None, which means all the instance will be on demand.
sparkInstanceBidPrice := Some("0.38")

sparkInstanceRole := "EMR_EC2_DefaultRole"

sparkS3LoggingFolder := Some("s3://aws-logs-xxxxxxxxxxxx-ap-northeast-1/elasticmapreduce/")

//The json configuration from S3 http://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-configure-apps.html
sparkS3JsonConfiguration := Some("s3://my-emr-bucket/my-emr-config.json")

sparkAdditionalApplications := Some(Seq("Presto", "Flink"))
```

## Other available commands

```
> sparkListClusters

> sparkTerminateCluster

> sparkSubmitJobWithMain mypackage.Main arg0 arg1 ...
```

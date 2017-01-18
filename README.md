# sbt-emr-spark

Run your [Spark on AWS EMR](http://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-launch.html) by sbt

## Getting started

1. Add sbt-emr-spark in `project/plugins.sbt`

   ```
   resolvers += Resolver.bintrayIvyRepo("pishen", "sbt-plugins")

   addSbtPlugin("net.pishen" % "sbt-emr-spark" % "0.2.0")
   ```
   (You may need to upgrade your sbt if you don't have the `bintrayIvyRepo` function.)

2. Prepare your `build.sbt`

   ```
   name := "sbt-emr-spark-test"

   scalaVersion := "2.11.8"

   libraryDependencies ++= Seq(
     "org.apache.spark" %% "spark-core" % "2.0.2" % "provided"
   )

   sparkAwsRegion := "ap-northeast-1"

   //(optional) Set the subnet id if you want to run Spark in VPC.
   sparkSubnetId := Some("subnet-xxxxxxxx")

   //(optional) Additional security groups that will be attached to master and slave's ec2.
   sparkAdditionalSecurityGroupIds := Some(Seq("sg-xxxxxxxx"))

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

4. Enter sbt and create your cluster

   ```
   > sparkCreateCluster
   ```

5. Submit your Spark application

   ```
   > sparkSubmitJob arg0 arg1 ...
   ```

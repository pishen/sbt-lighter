name := "simple"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.12"

sparkEmrClient := Build.emr

sparkS3Client := Build.s3

sparkAwsRegion := "ap-northeast-1"

sparkS3JarFolder := "s3://my-emr-bucket/my-emr-folder/"

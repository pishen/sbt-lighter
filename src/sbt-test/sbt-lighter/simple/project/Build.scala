import com.amazonaws.services.elasticmapreduce._
import com.amazonaws.services.elasticmapreduce.model._
import com.amazonaws.services.s3._
import com.amazonaws.services.s3.model._
import org.scalamock.scalatest.MockFactory

object Build extends MockFactory {
  val emr = stub[AmazonElasticMapReduce]

  inSequence {
    (emr
      .listClusters(_: ListClustersRequest))
      .when(*)
      .returns(new ListClustersResult())
    (emr
      .runJobFlow(_: RunJobFlowRequest))
      .when(*)
      .returns(
        new RunJobFlowResult().withJobFlowId("j-xxxlighterxxx")
      )
    (emr
      .listClusters(_: ListClustersRequest))
      .when(*)
      .anyNumberOfTimes()
      .returns(
        new ListClustersResult().withClusters(
          new ClusterSummary().withId("j-xxxlighterxxx")
        )
      )
  }

  (emr
    .addJobFlowSteps(_: AddJobFlowStepsRequest))
    .when(*)
    .returns(new AddJobFlowStepsResult())

  (emr
    .terminateJobFlows(_: TerminateJobFlowsRequest))
    .when(*)
    .returns(new TerminateJobFlowsResult())

  val s3 = stub[AmazonS3]

  (s3
    .putObject(_: PutObjectRequest))
    .when(*)
    .returns(new PutObjectResult())
}

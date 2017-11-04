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

import com.amazonaws.services.elasticmapreduce.model.{
  BootstrapActionConfig,
  ScriptBootstrapActionConfig
}
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import io.circe.parser.decode
import sbtemrspark.S3Url

import scala.collection.JavaConverters._
import scala.io.Source

case class EmrBootstrapActionConfig(
    Name: String,
    Path: String,
    Args: Option[Seq[String]]
) {

  def withArgs(props: String*) = {
    this.copy(Args = Some(props.toSeq))
  }

  def toAwsBootstrapActionConfig(): BootstrapActionConfig = {
    Some(new BootstrapActionConfig().withName(Name)).map { c =>
      Args
        .map(
          props =>
            c.withScriptBootstrapAction(
              new ScriptBootstrapActionConfig()
                .withPath(Path)
                .withArgs(props.toList.asJava)))
        .getOrElse(c.withScriptBootstrapAction(
          new ScriptBootstrapActionConfig().withPath(Path)))
    }.get
  }

}

object BootstrapConfig {
  def apply(name: String, path: String): EmrBootstrapActionConfig =
    EmrBootstrapActionConfig(name, path, None)

  def parseJson(jsonString: String) = decode[List[EmrBootstrapActionConfig]](jsonString)

  def parseJsonFromS3(s3Url: String)(
      implicit s3ClientBuilder: AmazonS3ClientBuilder) = {
    val s3JsonUrl = new S3Url(s3Url)
    val s3JsonInputStream = s3ClientBuilder
      .build()
      .getObject(s3JsonUrl.bucket, s3JsonUrl.key)
      .getObjectContent
    val jsonString = Source.fromInputStream(s3JsonInputStream).mkString
    parseJson(jsonString)
  }
}

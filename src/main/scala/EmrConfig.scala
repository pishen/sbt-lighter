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

import scala.collection.JavaConverters._
import scala.io.Source

import com.amazonaws.services.elasticmapreduce.model.Configuration
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import io.circe.generic.auto._
import io.circe.parser._

case class EmrConfig(
    Classification: String,
    Properties: Option[Map[String, String]],
    Configurations: Option[Seq[EmrConfig]]
) {
  def withProperties(props: (String, String)*) = {
    this.copy(Properties = Some(props.toMap))
  }

  def withEmrConfigs(configs: EmrConfig*) = {
    this.copy(Configurations = Some(configs))
  }

  def toAwsEmrConfig(): Configuration = {
    Some(new Configuration().withClassification(Classification))
      .map { c =>
        Properties.map(props => c.withProperties(props.asJava)).getOrElse(c)
      }
      .map { c =>
        Configurations
          .map { configs =>
            c.withConfigurations(configs.map(_.toAwsEmrConfig): _*)
          }
          .getOrElse(c)
      }
      .get
  }
}

object EmrConfig {
  def apply(classification: String): EmrConfig =
    EmrConfig(classification, None, None)

  def parseJson(jsonString: String) = decode[List[EmrConfig]](jsonString)
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

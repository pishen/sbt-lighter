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

package sbtlighter

import com.amazonaws.services.elasticmapreduce.model.{
  BootstrapActionConfig,
  ScriptBootstrapActionConfig
}
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import scala.collection.JavaConverters._

case class BootstrapAction(
  name: String,
  path: String,
  args: String*
) {
  def toAwsBootstrapActionConfig(): BootstrapActionConfig = {
    new BootstrapActionConfig()
      .withName(name)
      .withScriptBootstrapAction(
        new ScriptBootstrapActionConfig()
          .withPath(path)
          .withArgs(args.asJava))
  }
}
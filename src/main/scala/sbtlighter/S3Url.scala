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

class S3Url(url: String) {
  require(
    url.startsWith("s3://"),
    "S3 url should start with \"s3://\". Did you forget to set the sparkS3JarFolder key?"
  )

  val (bucket, key) = url.drop(5).split("/").toList match {
    case head :: Nil => (head, "")
    case head :: tail => (head, tail.mkString("/"))
    case _ => sys.error(s"unrecognized s3 url: $url")
  }

  def /(subPath: String) = {
    val newKey = if (key == "") subPath else key + "/" + subPath
    new S3Url(s"s3://$bucket/$newKey")
  }

  override def toString = s"s3://$bucket/$key"
}

object S3Url {
  def apply(url: String) = new S3Url(url)
}

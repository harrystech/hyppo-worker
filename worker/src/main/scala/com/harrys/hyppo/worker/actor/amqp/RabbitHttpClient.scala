package com.harrys.hyppo.worker.actor.amqp

import java.net.URLEncoder
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.rabbitmq.client.ConnectionFactory
import com.typesafe.scalalogging.Logger
import org.apache.commons.io.IOUtils
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet, HttpUriRequest}
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.auth.BasicScheme
import org.apache.http.impl.client._
import org.apache.http.message.BasicHeader
import org.apache.http.util.EntityUtils
import org.apache.http.{HttpHeaders, HttpHost}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions
import scala.concurrent.duration._

/**
 * Created by jpetty on 9/15/15.
 */
final class RabbitHttpClient(server: ConnectionFactory, port: Int, useSSL: Boolean = true) {
  private val log = Logger(LoggerFactory.getLogger(this.getClass))
  private val scheme  = if (useSSL) "https" else "http"
  private val baseURI = new URIBuilder().setScheme(scheme).setHost(server.getHost).setPort(port).build()

  def fetchQueueStatusInfo() : Seq[QueueStatusInfo] = {
    val requestURI = baseURI.resolve("/api/queues/" + URLEncoder.encode(server.getVirtualHost, "UTF-8"))

    performRequest(new HttpGet(requestURI)) { response =>
      import JsonMethods._
      implicit val formats = DefaultFormats
      val json   = parse(EntityUtils.toString(response.getEntity))
      val format = DateTimeFormatter.ofPattern("yyyy-MM-dd H:mm:ss")
      json.children.map(queue => {
        try {
          val name = (queue \ "name").extract[String]
          val size = (queue \ "messages").extract[Option[Int]].getOrElse(0)
          val rate = (queue \ "messages_details" \ "rate").extract[Option[Double]].getOrElse(0.0)
          val idle = (queue \ "idle_since").extract[Option[String]].map(LocalDateTime.parse(_, format))
          QueueStatusInfo(name, size, rate, idle.getOrElse(LocalDateTime.now()))
        } catch {
          case e: Exception =>
            throw new Exception(s"Failed to parse expected RabbitMQ structure from: ${compact(queue)}", e)
        }
      }).toSeq
    }
  }

  private def performRequest[T](request: HttpUriRequest)(handler: (CloseableHttpResponse) => T) : T = {
    val client  = createHttpClient()
    val context = HttpClientContext.create()

    val authCache = new BasicAuthCache()
    authCache.put(new HttpHost(server.getHost, port, scheme), new BasicScheme())
    context.setAuthCache(authCache)

    var response: CloseableHttpResponse = null

    try {
      response = client.execute(request, context)
      log.debug(s"[${request.getMethod}] ${response.getStatusLine.getStatusCode} - ${request.getURI.toString}")
      handler(response)
    } catch {
      case e: Exception =>
        log.error(s"[${request.getMethod}] - ${request.getURI.toString}", e)
        throw e
    } finally {
      IOUtils.closeQuietly(response)
      IOUtils.closeQuietly(client)
    }
  }

  private def createHttpClient() : CloseableHttpClient = {
    val credentials = new UsernamePasswordCredentials(server.getUsername, server.getPassword)

    val provider = new BasicCredentialsProvider()
    provider.setCredentials(new AuthScope(server.getHost, port), credentials)

    val headers = JavaConversions.asJavaCollection(Seq(new BasicHeader(HttpHeaders.ACCEPT, "application/json")))

    HttpClients.custom()
      .setDefaultRequestConfig(defaultRequestConfig.build())
      .setDefaultCredentialsProvider(provider)
      .setDefaultHeaders(headers)
      .build()
  }

  private def defaultRequestConfig : RequestConfig.Builder = {
    val connectTimeout = FiniteDuration(10, SECONDS)
    val socketTimeout  = FiniteDuration(30, SECONDS)
    RequestConfig.copy(RequestConfig.DEFAULT)
      .setConnectTimeout(connectTimeout.toMillis.toInt)
      .setSocketTimeout(socketTimeout.toMillis.toInt)
  }
}

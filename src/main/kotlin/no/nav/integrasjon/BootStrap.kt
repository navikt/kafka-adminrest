package no.nav.integrasjon

import io.ktor.application.call
import io.ktor.application.install
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.response.respondText
import io.ktor.response.respondWrite
import io.ktor.routing.Routing
import io.ktor.routing.get
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.exporter.common.TextFormat
import kotlinx.coroutines.experimental.delay
import kotlinx.coroutines.experimental.runBlocking
import mu.KotlinLogging
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.acl.AclBindingFilter
import org.apache.kafka.common.config.ConfigResource
import java.util.*
import java.util.concurrent.TimeUnit

object BootStrap {

    private val log = KotlinLogging.logger {  }

    // create default prometheus collector of metrics
    private val collectorRegistry = CollectorRegistry.defaultRegistry

    @Volatile var shutdownhookActive = false
    private val mainThread: Thread = Thread.currentThread()

    init {
        log.info { "Installing shutdown hook" }
        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                shutdownhookActive = true
                mainThread.join()
            }
        })
    }

    fun getTopics(adminClient: AdminClient): String =
            try {
                adminClient.listTopics().listings().get().map { it }.toString()
            }
            catch (e: Exception) {
                log.error { "Exception during fetch of topics - $e" }
                ""
            }


    fun getAcls(adminClient: AdminClient): String =
            try {
                adminClient.describeAcls(AclBindingFilter.ANY).values().get().toString()
            }
            catch (e: Exception) {
                log.error { "Exception during fetch of acls - $e" }
                ""
            }


    fun getCluster(adminClient: AdminClient): String =
            try {
                adminClient.describeCluster().nodes().get().toString()
            }
            catch (e: Exception) {
                log.error { "Exception during fetch of cluster info - $e" }
                ""
            }


    fun getBrokerConfig(adminClient: AdminClient, brokerID: String): String =
            try {
                adminClient.describeConfigs(mutableListOf(ConfigResource(ConfigResource.Type.BROKER,brokerID)))
                        .values()
                        .entries
                        .map { Pair(it.key,it.value.get()) }
                        .toString()
            }
            catch (e: Exception) {
                log.error { "Exception during fetch of broker config - $e" }
                ""
            }

    fun getTopicConfig(adminClient: AdminClient, topicName: String): String =
            try {
                adminClient.describeConfigs(
                        mutableListOf(
                                ConfigResource(ConfigResource.Type.TOPIC,topicName))
                )
                        .values()
                        .entries
                        .map { Pair(it.key,it.value.get()) }
                        .toString()
            }
            catch (e: Exception) {
                log.error { "Exception during fetch of topic config - $e" }
                ""
            }



    fun start(props: Properties) {

        log.info { "@start of bootstrap" }

        log.info { "Starting embedded REST server" }
        val eREST = embeddedServer(Netty, 8080){}.start()

        val REST_ENDPOINT = "/api/v1"

        try {
            runBlocking {

                AdminClient.create(props).use { adminClient ->

                    log.info { "Installing isAlive, /isReady and /prometheus routes" }
                    eREST.application.install(Routing) {
                        get("/isAlive") {
                            call.respondText("is alive", ContentType.Text.Plain)
                        }
                        get("/isReady") {
                            call.respondText("is ready", ContentType.Text.Plain)
                        }
                        get("/prometheus") {
                            val names = call.request.queryParameters.getAll("name[]")?.toSet() ?: setOf()
                            call.respondWrite(ContentType.parse(TextFormat.CONTENT_TYPE_004)) {
                                TextFormat.write004(
                                        this,
                                        collectorRegistry.filteredMetricFamilySamples(names))
                            }
                        }
                        get("$REST_ENDPOINT/topics") {

                            call.respondText(ContentType.Text.Plain, HttpStatusCode.OK) { getTopics(adminClient) }
                        }

                        get("$REST_ENDPOINT/topics/{topicName}/config") {

                            val topicName = call.parameters["topicName"] ?: throw IllegalArgumentException("Parameter topicName not found")
                            call.respondText(ContentType.Text.Plain, HttpStatusCode.OK) { getTopicConfig(adminClient,topicName) }
                        }

                        get("$REST_ENDPOINT/acls") {

                            call.respondText(ContentType.Text.Plain, HttpStatusCode.OK) { getAcls(adminClient) }
                        }

                        get("$REST_ENDPOINT/brokers") {

                            call.respondText(ContentType.Text.Plain, HttpStatusCode.OK) { getCluster(adminClient) }
                        }

                        get("$REST_ENDPOINT/brokers/{brokerID}") {
                            val brokerID = call.parameters["brokerID"] ?: throw IllegalArgumentException("Parameter brokerID not found")
                            call.respondText(ContentType.Text.Plain, HttpStatusCode.OK) { getBrokerConfig(adminClient,brokerID) }
                        }

                    }

                    while (!shutdownhookActive) delay(1_000)

                    if (shutdownhookActive) log.info { "Shutdown hook activated - preparing shutdown" }
                }
            }
        }
        catch (e: Exception) {
            log.error("Exception", e)
        }
        finally {
            eREST.stop(100,100, TimeUnit.MILLISECONDS)
            log.info { "@end of bootstrap" }
        }
    }
}
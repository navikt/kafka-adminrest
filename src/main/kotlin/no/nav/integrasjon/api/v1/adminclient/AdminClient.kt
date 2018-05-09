package no.nav.integrasjon.api.v1.adminclient

import io.ktor.application.call
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.response.respondText
import io.ktor.routing.Routing
import io.ktor.routing.get
import no.nav.integrasjon.api.v1.ACLS
import no.nav.integrasjon.api.v1.BROKERS
import no.nav.integrasjon.api.v1.TOPICS
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.acl.AclBindingFilter
import org.apache.kafka.common.config.ConfigResource

fun Routing.kafkaAPI(adminClient: AdminClient) {
    getBrokers(adminClient)
    getBrokerConfig(adminClient)
    getTopics(adminClient)
    getTopicConfig(adminClient)
    getACLS(adminClient)
}

fun Routing.getBrokers(adminClient: AdminClient) =
        get(BROKERS) {
            val result = try {
                adminClient.describeCluster().nodes().get().toString()
            }
            catch (e: Exception) { "" }

            call.respondText(ContentType.Text.Plain, HttpStatusCode.OK) { result }
        }

fun Routing.getBrokerConfig(adminClient: AdminClient) =
        get("$BROKERS/{brokerID}") {
            val brokerID = call.parameters["brokerID"] ?: throw IllegalArgumentException("Parameter brokerID not found")

            val result = try {
                adminClient.describeConfigs(mutableListOf(ConfigResource(ConfigResource.Type.BROKER,brokerID)))
                        .values()
                        .entries
                        .map { Pair(it.key,it.value.get()) }
                        .toString()
            }
            catch (e: Exception) { "" }

            call.respondText(ContentType.Text.Plain, HttpStatusCode.OK) { result }
        }

fun Routing.getTopics(adminClient: AdminClient) =
        get(TOPICS) {

            val result =  try {
                adminClient.listTopics().listings().get().map { it }.toString()
            }
            catch (e: Exception) { "" }

            call.respondText(ContentType.Text.Plain, HttpStatusCode.OK) { result }
        }

fun Routing.getTopicConfig(adminClient: AdminClient) =
        get("$TOPICS/{topicName}/config") {

            val topicName = call.parameters["topicName"] ?: throw IllegalArgumentException("Parameter topicName not found")

            val result = try {
                adminClient.describeConfigs(mutableListOf(ConfigResource(ConfigResource.Type.TOPIC,topicName)))
                        .values()
                        .entries
                        .map { Pair(it.key,it.value.get()) }
                        .toString()
            }
            catch (e: Exception) { "" }

            call.respondText(ContentType.Text.Plain, HttpStatusCode.OK) { result }
        }

fun Routing.getACLS(adminClient: AdminClient) =
        get(ACLS) {

            val result = try {
                adminClient.describeAcls(AclBindingFilter.ANY).values().get().toString()
            }
            catch (e: Exception) { "" }

            call.respondText(ContentType.Text.Plain, HttpStatusCode.OK) { result }
        }
package no.nav.integrasjon.api.v1.adminclient

import io.ktor.application.ApplicationCall
import io.ktor.application.call
import io.ktor.http.HttpStatusCode
import io.ktor.pipeline.PipelineContext
import io.ktor.response.respond
import io.ktor.routing.Routing
import io.ktor.routing.get
import no.nav.integrasjon.api.v1.ACLS
import no.nav.integrasjon.api.v1.BROKERS
import no.nav.integrasjon.api.v1.TOPICS
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.acl.AclBindingFilter
import org.apache.kafka.common.config.ConfigResource

// a wrapper for kafka api to be installed as routes
fun Routing.kafkaAPI(adminClient: AdminClient) {
    getBrokers(adminClient)
    getBrokerConfig(adminClient)
    getTopics(adminClient)
    getTopicConfig(adminClient)
    getACLS(adminClient)
}

// simple class for kafka exceptions
private data class KafkaError(val error: String)

// a wrapper for each call to AdminClient - used in routes
private suspend fun PipelineContext<Unit, ApplicationCall>.kafka(block: () -> Any) =
        try {
            call.respond(block())
        }
        catch (e: Exception) {
            call.respond(HttpStatusCode.ExceptionFailed, KafkaError("Could not get data from kafka env. - $e"))
        }

fun Routing.getBrokers(adminClient: AdminClient) =
        get(BROKERS) { kafka { adminClient.describeCluster().nodes().get() } }


fun Routing.getBrokerConfig(adminClient: AdminClient) =
        get("$BROKERS/{brokerID}") {
            kafka {
                adminClient.describeConfigs(mutableListOf(ConfigResource(
                    ConfigResource.Type.BROKER,
                    call.parameters["brokerID"])))
                    .values()
                    .entries
                    .map { Pair(it.key,it.value.get()) } }
        }

fun Routing.getTopics(adminClient: AdminClient) = get(TOPICS) { kafka { adminClient.listTopics().listings().get() } }

fun Routing.getTopicConfig(adminClient: AdminClient) =
        get("$TOPICS/{topicName}") {
            kafka {
                adminClient.describeConfigs(mutableListOf(ConfigResource(
                        ConfigResource.Type.TOPIC,
                        call.parameters["topicName"])))
                        .values()
                        .entries
                        .map { Pair(it.key,it.value.get()) }
            }
        }

fun Routing.getACLS(adminClient: AdminClient) =
        get(ACLS) { kafka { adminClient.describeAcls(AclBindingFilter.ANY).values().get() } }
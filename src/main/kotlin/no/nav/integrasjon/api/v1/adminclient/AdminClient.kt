package no.nav.integrasjon.api.v1.adminclient

import io.ktor.application.ApplicationCall
import io.ktor.application.call
import io.ktor.http.HttpStatusCode
import io.ktor.pipeline.PipelineContext
import io.ktor.request.receive
import io.ktor.response.respond
import io.ktor.routing.*
import kotlinx.coroutines.experimental.runBlocking
import no.nav.integrasjon.api.v1.ACLS
import no.nav.integrasjon.api.v1.AnError
import no.nav.integrasjon.api.v1.BROKERS
import no.nav.integrasjon.api.v1.TOPICS
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.Config
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.acl.*
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.resource.Resource
import org.apache.kafka.common.resource.ResourceFilter
import org.apache.kafka.common.resource.ResourceType

// a wrapper for kafka api to be installed as routes
fun Routing.kafkaAPI(adminClient: AdminClient) {
    getBrokers(adminClient)
    getBrokerConfig(adminClient)

    getTopics(adminClient)
    createNewTopic(adminClient)
    deleteTopic(adminClient)

    getTopicConfig(adminClient)
    updateTopicConfig(adminClient)

    getTopicAcls(adminClient)
    createNewTopicAcls(adminClient)
    deleteTopicAcls(adminClient)

    getACLS(adminClient)
}

// a wrapper for each call to AdminClient - used in routes
private suspend fun PipelineContext<Unit, ApplicationCall>.kafka(block: () -> Any) =
        try {
            call.respond(block())
        }
        catch (e: Exception) {
            call.respond(HttpStatusCode.ExceptionFailed, AnError("Exception happened - $e"))
        }

/**
 * See https://kafka.apache.org/10/javadoc/index.html?org/apache/kafka/clients/admin/AdminClient.html
 *
 * Implementation of routes below are just using the AdminClient API
 */

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

/**
 * Observe - json payload is only one new topic at a time
 * Example json payload: {"name":"test7","numPartitions":1,"replicationFactor":1}
 */
fun Routing.createNewTopic(adminClient: AdminClient) =
        post(TOPICS) {
            kafka {
                val newTopic = runBlocking { call.receive<NewTopic>() }
                // TODO - should we return new topic config instead? Reroute of use of admin client?
                adminClient.createTopics(mutableListOf(newTopic))
                        .values()
                        .entries
                        .map { Pair(it.key, it.value.get()) }
            }
        }

// TODO - need to clean up properly (delete related ACLs, Groups)
/**
 * Observe 1 - no payload, deletion of just one topic at a time
 * Observe 2 - will take ´a little bit of time´ before the deleted topics is not showing up in topics list
 */
fun Routing.deleteTopic(adminClient: AdminClient) =
        delete("$TOPICS/{topicName}") {
            kafka {
                adminClient.deleteTopics(listOf(call.parameters["topicName"]))
                        .values()
                        .entries
                        .map { Pair(it.key, it.value.get()) }
            }
        }

/**
 * NB! The AdminClient is listing a config independent of the topic exists or not...
 * TODO - should implement a test for existence
 */

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

/**
 * Observe - update of topic config with one ConfigEntry(name, value) at the time
 * Please use getTopicConfig first in order to get an idea of which entry to update
 * e.g. {"name": "retention.ms","value": "3600000"}
 */
fun Routing.updateTopicConfig(adminClient: AdminClient) =
        put("$TOPICS/{topicName}") {
            kafka {
                val configEntry = runBlocking { call.receive<ConfigEntry>() }
            adminClient.alterConfigs(
                    mutableMapOf(
                            ConfigResource(
                                    ConfigResource.Type.TOPIC,
                                    call.parameters["topicName"]) to Config(listOf(configEntry))))
                    .values()
                    .entries
                    .map { Pair(it.key,it.value.get()) }
            }
        }


fun Routing.getTopicAcls(adminClient: AdminClient) =
        get("$TOPICS/{topicName}/acls") {
            kafka {
                adminClient.describeAcls(
                        AclBindingFilter(
                                ResourceFilter(ResourceType.TOPIC, call.parameters["topicName"]),
                                AccessControlEntryFilter.ANY))
                        .values().get()
            }
        }

/*private enum class Role {
    @SerializedName("producer") PRODUCER,
    @SerializedName("consumer") CONSUMER
}*/

/**
 * E.g. {
"producerGroupName": "User:ktprodTest",
"consumerGroupName": "User:ktconsTest"
}
 */

private data class ACEntry(
        val producerGroupName: String,
        val consumerGroupName: String)

fun Routing.createNewTopicAcls(adminClient: AdminClient) =
        post("$TOPICS/{topicName}/acls") {
            kafka {
                val acEntry = runBlocking { call.receive<ACEntry>() }
                val rsrc = Resource(ResourceType.TOPIC, call.parameters["topicName"])

                val acls = mutableListOf(
                        AclBinding(
                                rsrc,
                                AccessControlEntry(
                                        acEntry.producerGroupName,
                                        "*",
                                        AclOperation.WRITE,
                                        AclPermissionType.ALLOW)),
                        AclBinding(
                                rsrc,
                                AccessControlEntry(
                                        acEntry.producerGroupName,
                                        "*",
                                        AclOperation.DESCRIBE,
                                        AclPermissionType.ALLOW)),
                        AclBinding(
                                rsrc,
                                AccessControlEntry(
                                        acEntry.consumerGroupName,
                                        "*",
                                        AclOperation.READ,
                                        AclPermissionType.ALLOW)),
                        AclBinding(
                                rsrc,
                                AccessControlEntry(
                                        acEntry.consumerGroupName,
                                        "*",
                                        AclOperation.DESCRIBE,
                                        AclPermissionType.ALLOW))
                        )

                adminClient.createAcls(acls.toList())
                        .values()
                        .entries
                        .map { Pair(it.key,it.value.get()) }
            }
        }

fun Routing.deleteTopicAcls(adminClient: AdminClient) =
        delete("$TOPICS/{topicName}/acls") {
            kafka {
                adminClient.deleteAcls(mutableListOf(AclBindingFilter(
                        ResourceFilter(ResourceType.TOPIC, call.parameters["topicName"]),
                        AccessControlEntryFilter.ANY)))
                        .values()
                        .entries
                        .map { Pair(it.key,it.value.get()) }
            }
        }


fun Routing.getACLS(adminClient: AdminClient) =
        get(ACLS) { kafka { adminClient.describeAcls(AclBindingFilter.ANY).values().get() } }
package no.nav.integrasjon.api.v1

import io.ktor.application.application
import io.ktor.application.call
import io.ktor.auth.authenticate
import io.ktor.auth.authentication
import io.ktor.request.receive
import io.ktor.routing.*
import kotlinx.coroutines.experimental.runBlocking
import no.nav.integrasjon.AUTHENTICATION_BASIC
import no.nav.integrasjon.FasitProperties
import no.nav.integrasjon.ldap.LDAPBase
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
fun Routing.topicsAPI(adminClient: AdminClient, config: FasitProperties) {

    getTopics(adminClient)
    createNewTopic(adminClient, config)
    deleteTopic(adminClient)

    getTopicConfig(adminClient)
    updateTopicConfig(adminClient)

    getTopicAcls(adminClient)
    //getTopicGroups(config) TODO - must implement
    //deleteTopicAcls(adminClient) // TODO - to be deleted

}

fun Routing.getTopics(adminClient: AdminClient) =
        get(TOPICS) {
            respondAndCatch {
                adminClient.listTopics().listings().get()
            }
        }


/**
 * Observe - json payload is only one new topic at a time - NewTopic
 * e.g. {"name":"aTopicName","numPartitions":1,"replicationFactor":1}
 */
fun Routing.createNewTopic(adminClient: AdminClient, config: FasitProperties) =
        authenticate(AUTHENTICATION_BASIC) {
            post(TOPICS) {
                respondAndCatch {
                    val newTopic = runBlocking { call.receive<NewTopic>() }
                    val logEntry = "Topic creation by ${this.context.authentication.principal} - $newTopic"
                    application.environment.log.info(logEntry)
                        adminClient.createTopics(mutableListOf(newTopic))
                                .values()
                                .entries.first()
                                //.map { Pair(it.key, it.value.get()) }
                                .also { createGroupsAndAcls(adminClient, config, newTopic.name()) }
                }
            }
        }

private fun createGroupsAndAcls(adminClient: AdminClient, config: FasitProperties, topicName: String) {

    val rsrc = Resource(ResourceType.TOPIC, topicName)

    val ldapGroups = LDAPBase(config).use { ldap -> ldap.createKafkaGroups(topicName) }

    val acls = ldapGroups.map { kafkaGroup ->
        val principal = "Group:${kafkaGroup.name}"
        listOf(
                AclBinding(
                        rsrc,
                        AccessControlEntry(
                                principal,
                                "*",
                                AclOperation.WRITE,
                                AclPermissionType.ALLOW)),
                AclBinding(
                        rsrc,
                        AccessControlEntry(
                                principal,
                                "*",
                                AclOperation.DESCRIBE,
                                AclPermissionType.ALLOW))
        )
    }.flatMap { it }

    adminClient.createAcls(acls)
            .values()
            .entries
            .map { Pair(it.key,it.value.get()) }

}

// TODO - need to clean up properly (delete related ACLs, Groups)
/**
 * Observe 1 - no payload, deletion of just one topic at a time
 * Observe 2 - will take ´a little bit of time´ before the deleted topics is not showing up in topics list
 */
fun Routing.deleteTopic(adminClient: AdminClient) =
        delete("$TOPICS/{topicName}") {
            respondAndCatch {
                adminClient.deleteTopics(listOf(call.parameters["topicName"]))
                        .values()
                        .entries
                        .map { Pair(it.key, it.value.get()) }
                        //.also { deleteGroupsAndAcls() }
            }
        }

/**
 * NB! The AdminClient is listing a config independent of the topic exists or not...
 * TODO - should implement a test for existence
 */

fun Routing.getTopicConfig(adminClient: AdminClient) =
        get("$TOPICS/{topicName}") {
            respondAndCatch {
                adminClient.describeConfigs(mutableListOf(ConfigResource(
                        ConfigResource.Type.TOPIC,
                        call.parameters["topicName"])))
                        .values()
                        .entries
                        .map { Pair(it.key,it.value.get()) }
            }
        }

// TODO - restrict the update options...
/**
 * Observe - update of topic config with one ConfigEntry(name, value) at the time
 * Please use getTopicConfig first in order to get an idea of which entry to update
 * e.g. {"name": "retention.ms","value": "3600000"}
 */
fun Routing.updateTopicConfig(adminClient: AdminClient) =
        put("$TOPICS/{topicName}") {
            respondAndCatch {
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
            respondAndCatch {
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

/*fun Routing.createGroupsAndAcls(adminClient: AdminClient) =
        post("$TOPICS/{topicName}/acls") {
            respondAndCatch {
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
        }*/



/*fun Routing.deleteTopicAcls(adminClient: AdminClient) =
        delete("$TOPICS/{topicName}/acls") {
            respondAndCatch {
                adminClient.deleteAcls(mutableListOf(AclBindingFilter(
                        ResourceFilter(ResourceType.TOPIC, call.parameters["topicName"]),
                        AccessControlEntryFilter.ANY)))
                        .values()
                        .entries
                        .map { Pair(it.key,it.value.get()) }
            }
        }*/

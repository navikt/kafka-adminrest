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
import org.apache.kafka.clients.admin.*
import org.apache.kafka.common.acl.*
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.resource.Resource
import org.apache.kafka.common.resource.ResourceFilter
import org.apache.kafka.common.resource.ResourceType

// a wrapper for kafka api to be installed as routes
fun Routing.topicsAPI(adminClient: AdminClient, config: FasitProperties) {

    getTopics(adminClient)
    createNewTopic(adminClient, config)
    deleteTopic(adminClient, config)

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
                    val logEntry = "Topic creation request by ${this.context.authentication.principal} - $newTopic"
                    application.environment.log.info(logEntry)

                    // create the topic, and if successful, with LDAP groups and ACLs
                    adminClient.createTopics(mutableListOf(newTopic)).all().get().let {

                        application.environment.log.info("Topic ${newTopic.name()} created")

                        // create kafka groups in ldap
                        val groupsResult = LDAPBase(config).use { ldap -> ldap.createKafkaGroups(newTopic.name()) }

                        // create ACLs based on kafka groups in LDAP
                        val rsrc = Resource(ResourceType.TOPIC, newTopic.name())
                        val acls = groupsResult.map { kafkaGroup ->
                            val principal = "Group:${kafkaGroup.name}"
                            val host = "*"
                            listOf(
                                    AclBinding(
                                            rsrc,
                                            AccessControlEntry(
                                                    principal,
                                                    host,
                                                    when(kafkaGroup.groupType) {
                                                        LDAPBase.KafkaGroupType.PRODUCER -> AclOperation.WRITE
                                                        LDAPBase.KafkaGroupType.CONSUMER -> AclOperation.READ
                                                    },
                                                    AclPermissionType.ALLOW)),
                                    AclBinding(
                                            rsrc,
                                            AccessControlEntry(
                                                    principal,
                                                    host,
                                                    AclOperation.DESCRIBE,
                                                    AclPermissionType.ALLOW))
                            )
                        }.flatMap { it }

                        application.environment.log.info("Create ACLs request: $acls")

                        val aclsResult = try {
                            adminClient.createAcls(acls).all().get()
                            application.environment.log.info("ACLs $acls created")
                            Pair(acls, "Created")
                        }
                        catch (e: Exception) {
                            Pair(acls, "Failure, $e")
                        }

                        Triple(Pair("Topic ${newTopic.name()}","Created"), groupsResult, aclsResult)
                    }
                }
            }
        }

/**
 * Observe 1 - no payload, deletion of just one topic at a time
 * Observe 2 - will take ´a little bit of time´ before the deleted topics is not showing up in topics list
 */
fun Routing.deleteTopic(adminClient: AdminClient, config: FasitProperties) =
        authenticate(AUTHENTICATION_BASIC) {
            delete("$TOPICS/{topicName}") {
                respondAndCatch {

                    // elvis paradox, should not happen because then we should not be here...
                    val topicName = call.parameters["topicName"] ?: "<no topic>"

                    val logEntry = "Topic deletion request by ${this.context.authentication.principal} - $topicName"
                    application.environment.log.info(logEntry)

                    // delete ACLs
                    val aclsDeletion = AclBindingFilter(
                            ResourceFilter(ResourceType.TOPIC, topicName),
                            AccessControlEntryFilter.ANY
                    )

                    application.environment.log.info("Delete ACLs request: $aclsDeletion")

                    val aclsResult = try {
                        adminClient.deleteAcls(mutableListOf(aclsDeletion)).all().get()
                        application.environment.log.info("ACLs $aclsDeletion deleted")
                        Pair(aclsDeletion, "Deleted")
                    } catch (e: Exception) {
                        Pair(aclsDeletion, "Failure, $e")
                    }

                    // delete related kafka ldap groups
                    val groupsResult = LDAPBase(config).use { ldap -> ldap.deleteKafkaGroups(topicName) }

                    // delete the topic itself
                    val topicResult = try {
                        adminClient.deleteTopics(listOf(topicName)).all().get()
                        Pair("Topic $topicName", "Deleted")
                    } catch (e: Exception) {
                        Pair("Topic $topicName", "Failure, $e")
                    }

                    Triple(topicResult, groupsResult, aclsResult)
                }
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
 * Observe - update of topic config with one ConfigEntry(name, value) at a time
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

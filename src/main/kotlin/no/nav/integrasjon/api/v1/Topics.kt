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
import no.nav.integrasjon.ldap.LDAPGroup
import org.apache.kafka.clients.admin.*
import org.apache.kafka.common.KafkaException
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

    getTopicGroups(config)
    updateTopicGroup(config)
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
                        val groupsResult = LDAPGroup(config).use { ldap -> ldap.createKafkaGroups(newTopic.name()) }

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
                                                        LDAPGroup.KafkaGroupType.PRODUCER -> AclOperation.WRITE
                                                        LDAPGroup.KafkaGroupType.CONSUMER -> AclOperation.READ
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
                    val acls = AclBindingFilter(
                            ResourceFilter(ResourceType.TOPIC, topicName),
                            AccessControlEntryFilter.ANY
                    )

                    application.environment.log.info("Delete ACLs request: $acls")

                    val aclsResult = try {
                        adminClient.deleteAcls(mutableListOf(acls)).all().get()
                        application.environment.log.info("ACLs $acls deleted")
                        Pair(acls, "Deleted")
                    } catch (e: Exception) {
                        Pair(acls, "Failure, $e")
                    }

                    // delete related kafka ldap groups
                    val groupsResult = LDAPGroup(config).use { ldap -> ldap.deleteKafkaGroups(topicName) }

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

fun Routing.getTopicConfig(adminClient: AdminClient) =
        get("$TOPICS/{topicName}") {
            respondAndCatch {

                // elvis paradox, should not happen because then we should not be here...
                val topicName = call.parameters["topicName"] ?: "<no topic>"

                // NB! AdminClient is listing a default config independent of topic exists or not, verify!
                val existingTopics = adminClient.listTopics().listings().get().map { it.name() }

                if (existingTopics.contains(topicName))
                    adminClient.describeConfigs(
                            mutableListOf(
                                    ConfigResource(
                                            ConfigResource.Type.TOPIC,
                                            topicName
                                    )
                            )
                    ).all().get()
                else
                    Pair("Result", "Topic $topicName does not exist")
            }
        }

/**
 * Observe - update of topic config with one ConfigEntry(name, value) at a time
 * Please use getTopicConfig first in order to get an idea of which entry to update with related unit of value
 * e.g. {"name": "retention.ms","value": "3600000"}
 *
 * Observe - only a few entries are allowed to update automatically
 */

enum class AllowedConfigEntries(val entryName: String) {
    RETENTION_MS("retention.ms")
    // TODO - will be enhanced with suitable set of config entries
}

fun Routing.updateTopicConfig(adminClient: AdminClient) =
        authenticate(AUTHENTICATION_BASIC) {
            put("$TOPICS/{topicName}") {
                respondAndCatch {
                    val topicName = call.parameters["topicName"] ?: "<no topic>"
                    val configEntry = runBlocking { call.receive<ConfigEntry>() }

                    // check whether configEntry is allowed
                    if (!AllowedConfigEntries.values().map { it.entryName }.contains(configEntry.name()))
                        throw KafkaException("configEntry ${configEntry.name()} is not allowed to update automatically")

                    val logEntry = "Topic config. update request by ${this.context.authentication.principal} - $topicName"
                    application.environment.log.info(logEntry)

                    val configResource = ConfigResource(ConfigResource.Type.TOPIC, topicName)
                    val configReq = mapOf(configResource to Config(listOf(configEntry)))

                    application.environment.log.info("Update topic config request: $configReq")

                    // NB! .all is throwing error... Use of future for specific entry instead
                    adminClient.alterConfigs(configReq)
                            //.all()
                            .values()[configResource]
                            ?.get() ?: Pair("$configEntry","has been updated")
                }
            }
        }


fun Routing.getTopicAcls(adminClient: AdminClient) =
        get("$TOPICS/{topicName}/acls") {
            respondAndCatch {

                // elvis paradox, should not happen because then we should not be here...
                val topicName = call.parameters["topicName"] ?: "<no topic>"

                adminClient.describeAcls(
                        AclBindingFilter(
                                ResourceFilter(ResourceType.TOPIC, topicName),
                                AccessControlEntryFilter.ANY)
                )
                        .values()
                        .get()
            }
        }

fun Routing.getTopicGroups(config: FasitProperties) =
        get("$TOPICS/{topicName}/groups") {
            respondAndCatch {

                // elvis paradox, should not happen because then we should not be here...
                val topicName = call.parameters["topicName"] ?: "<no topic>"

                LDAPGroup(config).use { ldap -> ldap.getKafkaGroupsAndMembers(topicName) }
            }
        }

fun Routing.updateTopicGroup(config: FasitProperties) =
        authenticate(AUTHENTICATION_BASIC) {
            put("$TOPICS/{topicName}/groups") {
                respondAndCatch {

                    // elvis paradox, should not happen because then we should not be here...
                    val topicName = call.parameters["topicName"] ?: "<no topic>"
                    val updateEntry = runBlocking { call.receive<LDAPGroup.UpdateKafkaGroupMember>() }
                    val groupName = LDAPGroup.toGroupName(updateEntry.role.prefix, topicName)

                    val logEntry = "Topic group membership update request by " +
                            "${this.context.authentication.principal} - $groupName "
                    application.environment.log.info(logEntry)

                    LDAPGroup(config)
                            .use { ldap -> ldap.updateKafkaGroupMembership(groupName, updateEntry) }
                            .also { application.environment.log.info("$groupName has been updated") }
                }
            }
        }




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
import org.apache.kafka.common.acl.*
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.resource.Resource
import org.apache.kafka.common.resource.ResourceFilter
import org.apache.kafka.common.resource.ResourceType

/**
 * Topic API
 *
 * - get all topics (except hidden) in cluster
 * - create new topic, automatic creation of groups and topic ACLs
 * - delete topic, automatic deletion of topic ACLs and LDAP groups
 *
 * - get topic configuration
 * - update topic configuration, although, restricted to a few configuration elements
 *
 * - get topic access control lists
 *
 * - get topic groups
 * - update topic group membership (add/remove member)
 */

// a wrapper for this api to be installed as routes
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

/**
 * GET https://<host>/api/v1/topics
 *
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/admin/AdminClient.html#listTopics-org.apache.kafka.clients.admin.ListTopicsOptions-
 *
 * Returns a map of topic names to org.apache.kafka.clients.admin.TopicListing
 *
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/admin/TopicListing.html
 */
fun Routing.getTopics(adminClient: AdminClient) =
        get(TOPICS) {
            respondCatch {
                adminClient.listTopics().namesToListings().get()
            }
        }

/**
 * POST https://<host>/api/v1/topics
 *
 * Observe - json payload is only one new topic at a time - ANewTopic
 *
 * e.g. {"name":"aTopicName","numPartitions":3}
 *
 * Observe that we need to create kafka NewTopic where the missing piece is the replication factor
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/admin/NewTopic.html
 *
 * The system will get the default.replication.factor from the 1. broker configuration. In that way the replication
 * factor will be correct across test, preprod and production environment. The latter has higher repl. factor in order
 * to guarantee HA even if one data center goes down
 *
 * Observe requirement for authentication, e.g. n145821 and pwd - the classic stuff
 *
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/admin/AdminClient.html#createTopics-java.util.Collection-
 *
 * Iff topic creation is successful,
 * - related producer and consumer LDAP groups will be created
 * - related ACLs will be created, producer group with WRITE&DESCRIBE, consumer group with READ&DESCRIBE
 *
 * Returns a triplet<
 *      Pair<"Topic aName", "Created">,
 *      Collection<KafkaGroup>,
 *      Pair<Collection<AclBinding>, "Created">
 *     >
 *
 *  In case of failure for one of the LDAP groups, the LDAPGroup::KafkaGroup.result contains LDAPResult
 *  In case of failure for ACLs, "Failure, <exception> "instead of "Created"
 */

// get the default replication factor from 1st broker configuration. Due to puppet, consistency across brokers in a
// kafka cluster
private fun getDefaultReplicationFactor(adminClient: AdminClient): Short =
        adminClient.describeCluster().nodes().get().first().let { node ->
            adminClient.describeConfigs(
                    listOf(
                            ConfigResource(
                                    ConfigResource.Type.BROKER,
                                    node.idString()
                            )
                    )
            ).all().get().values.first().get("default.replication.factor").value().toShort()
        }

// simplified version of kafka NewTopic
internal data class ANewTopic(val name: String, val numPartitions:Int)

// extension function for validating a topic name and that the future group names are of valid length
private fun String.isValid(): Boolean =
        this.map { c ->
            when {
                (c in 'A'..'Z' || c in 'a'..'z' || c == '-' || c in '0'..'9') -> true
                else -> false
            }
        }.all { it } && LDAPGroup.validGroupLength(this)


fun Routing.createNewTopic(adminClient: AdminClient, config: FasitProperties) =
        authenticate(AUTHENTICATION_BASIC) {
            post(TOPICS) {
                respondCatch {
                    val aNewTopic = runBlocking { call.receive<ANewTopic>() }

                    if (!aNewTopic.name.isValid())
                        throw Exception("Invalid topic name - $aNewTopic.name. " +
                                "Must contain [a..z]||[A..Z]||[0..9]||'-' only " +
                                "&& + length ≤ ${LDAPGroup.maxTopicNameLength()}")

                    // create kafka NewTopic by getting the default replication factor from broker config
                    val newTopic = NewTopic(
                            aNewTopic.name,
                            aNewTopic.numPartitions,
                            getDefaultReplicationFactor(adminClient)
                    )

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
                                                        LDAPGroup.Companion.KafkaGroupType.PRODUCER -> AclOperation.WRITE
                                                        LDAPGroup.Companion.KafkaGroupType.CONSUMER -> AclOperation.READ
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

                        application.environment.log.info("ACLs create request: $acls")

                        val aclsResult = try {
                            adminClient.createAcls(acls).all().get()
                            application.environment.log.info("ACLs created - $acls")
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
 * DELETE https://<host>/api/v1/topics/{topicName}
 *
 * Observe - no json payload, deletion of just one topic at a time
 * Observe - will take ´a little bit of time´ before the deleted topics is not showing up in topics list
 * Observe requirement for authentication, e.g. n145821 and pwd - the classic stuff
 *
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/admin/AdminClient.html#deleteTopics-java.util.Collection-
 *
 * Stepwise
 * - delete all ACLs related to topic
 * - delete related producer and consumer LDAP groups
 * - delete topic
 *
 * Returns a triplet<
 *      Pair<"Topic aName", "Deleted">,
 *      Collection<KafkaGroup>,
 *      Pair<Collection<AclBindingFilter>, "Deleted">
 *     >
 *
 *  In case of failure for one of the LDAP groups, the LDAPGroup::KafkaGroup.result contains LDAPResult
 *  In case of failure for ACLs, "Failure, <exception> "instead of "Deleted"
 */
fun Routing.deleteTopic(adminClient: AdminClient, config: FasitProperties) =
        authenticate(AUTHENTICATION_BASIC) {
            delete("$TOPICS/{topicName}") {
                respondCatch {

                    // elvis paradox, should not happen because then we should not be here...
                    val topicName = call.parameters["topicName"] ?: "<no topic>"

                    val logEntry = "Topic deletion request by ${this.context.authentication.principal} - $topicName"
                    application.environment.log.info(logEntry)

                    // delete ACLs
                    val acls = AclBindingFilter(
                            ResourceFilter(ResourceType.TOPIC, topicName),
                            AccessControlEntryFilter.ANY
                    )

                    application.environment.log.info("ACLs delete request: $acls")

                    val aclsResult = try {
                        adminClient.deleteAcls(mutableListOf(acls)).all().get()
                        application.environment.log.info("ACLs deleted - $acls")
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

/**
 * GET https://<host>/api/v1/topics/{topicName}
 *
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/admin/AdminClient.html#describeConfigs-java.util.Collection-
 *
 * Returns a map of org.apache.kafka.common.config.ConfigResource to org.apache.kafka.clients.admin.Config
 *
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/common/config/ConfigResource.html
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/admin/Config.html
 */
fun Routing.getTopicConfig(adminClient: AdminClient) =
        get("$TOPICS/{topicName}") {
            respondCatch {

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
 * PUT https://<host>/api/v1/topics/{topicName}
 *
 * Observe - json payload is only one new config entry at a time - ConfigEntry
 * Observe requirement for authentication, e.g. n145821 and pwd - the classic stuff
 *
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/admin/ConfigEntry.html
 * e.g. {"name": "retention.ms","value": "3600000"}
 *
 * Please use getTopicConfig first in order to get an idea of which entry to update with related value of unit
 *
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/admin/AdminClient.html#alterConfigs-java.util.Map-
 *
 * Returns a Pair<configEntry, "has been updated">
 */

// will be enhanced with suitable set of config entries
enum class AllowedConfigEntries(val entryName: String) {
    RETENTION_MS("retention.ms"),
    RETENTION_BYTES("retention.bytes"),
    CLEANUP_POLICY("cleanup.policy")
}

fun Routing.updateTopicConfig(adminClient: AdminClient) =
        authenticate(AUTHENTICATION_BASIC) {
            put("$TOPICS/{topicName}") {
                respondCatch {

                    val topicName = call.parameters["topicName"] ?: "<no topic>"
                    val configEntry = runBlocking { call.receive<ConfigEntry>() }

                    // check whether configEntry is allowed
                    if (!AllowedConfigEntries.values().map { it.entryName }.contains(configEntry.name()))
                        throw Exception("configEntry ${configEntry.name()} is not allowed to update automatically")

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

/**
 * GET https://<host>/api/v1/topics/{topicName}/acls
 *
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/admin/AdminClient.html#describeAcls-org.apache.kafka.common.acl.AclBindingFilter-
 *
 * Returns a collection of org.apache.kafka.common.acl.AclBinding
 *
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/common/acl/AclBinding.html
 */
fun Routing.getTopicAcls(adminClient: AdminClient) =
        get("$TOPICS/{topicName}/acls") {
            respondCatch {

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

/**
 * GET https://<host>/api/v1/topics/{topicName}/groups
 *
 * See LDAPGroup::getKafkaGroupsAndMembers
 *
 * Returns a collection of LDAPGroup.Companion.KafkaGroup
 */
fun Routing.getTopicGroups(config: FasitProperties) =
        get("$TOPICS/{topicName}/groups") {
            respondCatch {

                // elvis paradox, should not happen because then we should not be here...
                val topicName = call.parameters["topicName"] ?: "<no topic>"

                LDAPGroup(config).use { ldap -> ldap.getKafkaGroupsAndMembers(topicName) }
            }
        }

/**
 * PUT https://<host>/api/v1/topics/{topicName}/groups
 *
 * Observe - json payload is only one add/remove member at a time - LDAPGroup.Companion.UpdateKafkaGroupMember
 * Observe requirement for authentication, e.g. n145821 and pwd - the classic stuff
 *
 * e.g. {"role": "producer","operation": "add","member":"srvkafkaproducer"}
 *
 * See LDAPGroup::updateKafkaGroupMembership
 *
 * Returns LDAPResult
 */
fun Routing.updateTopicGroup(config: FasitProperties) =
        authenticate(AUTHENTICATION_BASIC) {
            put("$TOPICS/{topicName}/groups") {
                respondCatch {

                    // elvis paradox, should not happen because then we should not be here...
                    val topicName = call.parameters["topicName"] ?: "<no topic>"
                    val updateEntry = runBlocking { call.receive<LDAPGroup.Companion.UpdateKafkaGroupMember>() }
                    val logEntry = "Topic group membership update request by " +
                            "${this.context.authentication.principal} - $topicName "

                    application.environment.log.info(logEntry)

                    LDAPGroup(config)
                            .use { ldap -> ldap.updateKafkaGroupMembership(topicName, updateEntry) }
                            .also { application.environment.log.info("$topicName's group has been updated") }
                }
            }
        }




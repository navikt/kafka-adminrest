package no.nav.integrasjon.api.v1

import com.unboundid.ldap.sdk.ResultCode
import io.ktor.application.application
import io.ktor.application.call
import io.ktor.auth.UserIdPrincipal
import io.ktor.auth.authentication
import io.ktor.auth.principal
import io.ktor.http.HttpStatusCode
import io.ktor.locations.Location
import io.ktor.response.respond
import io.ktor.routing.Routing
import no.nav.integrasjon.EXCEPTION
import no.nav.integrasjon.FasitProperties
import no.nav.integrasjon.api.nais.client.SERVICES_ERR_K
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.BasicAuthSecurity
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.Group
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.badRequest
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.delete
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.failed
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.get
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.ok
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.post
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.put
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.responds
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.securityAndReponds
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.serviceUnavailable
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.unAuthorized
import no.nav.integrasjon.ldap.KafkaGroup
import no.nav.integrasjon.ldap.KafkaGroupType
import no.nav.integrasjon.ldap.LDAPGroup
import no.nav.integrasjon.ldap.SLDAPResult
import no.nav.integrasjon.ldap.UpdateKafkaGroupMember
import no.nav.integrasjon.ldap.intoAcls
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.Config
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.acl.AccessControlEntryFilter
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.acl.AclBindingFilter
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.ResourcePatternFilter
import org.apache.kafka.common.resource.ResourceType
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

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
fun Routing.topicsAPI(adminClient: AdminClient?, config: FasitProperties) {

    getTopics(adminClient)

/*    createNewTopic(adminClient, config)
    deleteTopic(adminClient, config)

    getTopicConfig(adminClient)
    updateTopicConfig(adminClient, config)

    getTopicAcls(adminClient)

    getTopicGroups(config)
    updateTopicGroup(config)*/
}

private const val swGroup = "Topics"

/**
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/admin/AdminClient.html#listTopics-org.apache.kafka.clients.admin.ListTopicsOptions-
 */

@Group(swGroup)
@Location(TOPICS)
class GetTopics

data class GetTopicsModel(val topics: List<String>)

fun Routing.getTopics(adminClient: AdminClient?) =
        get<GetTopics>("all topics".responds(ok<GetTopicsModel>(), serviceUnavailable<AnError>())) {
            respondOrServiceUnavailable {

                val topics = adminClient
                        ?.listTopics()
                        ?.names()
                        ?.get(KAFKA_TIMEOUT, TimeUnit.MILLISECONDS)
                        ?.toList()
                        ?: throw Exception(SERVICES_ERR_K)

                GetTopicsModel(topics)
            }
        }

/**
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/admin/NewTopic.html
 *
 * The system will get the default.replication.factor from the 1. broker configuration. In that way the replication
 * factor will be correct across test, preprod and production environment. The latter has higher repl. factor in order
 * to guarantee HA even if one data center goes down
 *
 */

// get the default replication factor from 1st broker configuration. Due to puppet, consistency across brokers in a
// kafka cluster
fun getDefaultReplicationFactor(adminClient: AdminClient): Short =
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

private val topicPattern: Pattern = Pattern.compile("[A-Za-z0-9-]+")
// extension function for validating a topic name and that the future group names are of valid length
fun String.isValidTopicName(): Boolean = topicPattern.matcher(this).matches() &&
        LDAPGroup.validGroupLength(this)

@Group(swGroup)
@Location(TOPICS)
class PostTopic
data class PostTopicBody(val name: String, val numPartitions: Int = 1)

data class PostTopicModel(
    val topicStatus: String,
    val groupsStatus: List<KafkaGroup>,
    val aclStatus: String
)

fun Routing.createNewTopic(adminClient: AdminClient, config: FasitProperties) =
        post<PostTopic, PostTopicBody>(
                "new topic. The authenticated user becomes member of KM-{newTopic}. Please add other team members"
                        .securityAndReponds(
                                BasicAuthSecurity(),
                                ok<PostTopicModel>(),
                                serviceUnavailable<AnError>(),
                                badRequest<AnError>(),
                                unAuthorized<Unit>())) { _, body ->

            val currentUser = call.principal<UserIdPrincipal>()!!.name
            val logEntry = "Topic creation request by $currentUser - $body"
            application.environment.log.info(logEntry)

            /**
             * Rule 1 - user must exist in current ldap environment in order to be part of group KM-<topic>
             */

            try { !LDAPGroup(config).use { ldap -> ldap.userExists(currentUser) }
            } catch (e: Exception) {
                call.respond(HttpStatusCode.ServiceUnavailable, AnError(
                        "authenticated user $currentUser doesn't exist as NAV ident or " +
                                "service user in current LDAP domain, cannot be manager of topic")
                )
                return@post
            }

            /**
             * Rule 2 - must be a valid topic name, limited by group name length in LDAP and type of characters
             */

            if (!body.name.isValidTopicName()) {
                call.respond(HttpStatusCode.BadRequest, AnError(
                        "Invalid topic name - $body.name. " +
                                "Must contain [a..z]||[A..Z]||[0..9]||'-' only " +
                                "&& + length ≤ ${LDAPGroup.maxTopicNameLength()}")
                )
                return@post
            }

            /**
             * Rule 3 - use default replication factor from kafka cluster
             * test and preprod has 3 brokers
             * production has 6 brokers
             *
             */

            val repFactorError = (-1).toShort()
            val defaultRepFactor = try { getDefaultReplicationFactor(adminClient) } catch (e: Exception) { repFactorError }

            if (defaultRepFactor == repFactorError) {
                call.respond(HttpStatusCode.ServiceUnavailable, AnError(
                        "Could not get replicationFactor for topic from Kafka")
                )
                return@post
            }

            val newTopic = NewTopic(body.name, body.numPartitions, defaultRepFactor)

            val (topicIsOk, topicResult) = try {
                adminClient.createTopics(mutableListOf(newTopic)).all().get()
                application.environment.log.info("Topic created - $newTopic")
                Pair(true, "created topic $newTopic")
            } catch (e: Exception) {
                // TODO should have warning for topcis already exists
                application.environment.log.error("$EXCEPTION topic create request $newTopic - $e")
                Pair(false, "failure for topic $newTopic creation, $e")
            }

            val groupsResult = LDAPGroup(config).use { ldap -> ldap.createKafkaGroups(newTopic.name(), currentUser) }

            val groupsAreOk = groupsResult
                    .asSequence()
                    .map { it.ldapResult.resultCode }
                    .all { it == ResultCode.SUCCESS }

            // create ACLs based on kafka groups in LDAP, except manager group KM-
            val acls = groupsResult.asSequence()
                    .filter { it.type != KafkaGroupType.MANAGER }
                    .map { kafkaGroup -> kafkaGroup.type.intoAcls(newTopic.name()) }
                    .flatten()

            application.environment.log.info("ACLs create request: $acls")

            val (aclsAreOk, aclsResult) = try {
                adminClient.createAcls(acls.toList()).all().get()
                application.environment.log.info("ACLs created - $acls")
                Pair(true, "created $acls")
            } catch (e: Exception) {
                application.environment.log.error("$EXCEPTION ACLs create request $acls - $e")
                Pair(false, "failure for $acls creation, $e")
            }

            val errorMsg = "Topic: $topicResult " +
                    "Groups: ${groupsResult.map { it.ldapResult.message }} " +
                    "ACLs: $aclsResult"

            when (topicIsOk && groupsAreOk && aclsAreOk) {
                true -> call.respond(PostTopicModel(topicResult, groupsResult, aclsResult))
                false -> call.respond(HttpStatusCode.ServiceUnavailable, errorMsg)
            }
        }

/**
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/admin/AdminClient.html#deleteTopics-java.util.Collection-
 */
@Group(swGroup)
@Location("$TOPICS/{topicName}")
data class DeleteTopic(val topicName: String)

data class DeleteTopicModel(
    val topicStatus: String,
    val groupsStatus: List<KafkaGroup>,
    val aclStatus: String
)

fun Routing.deleteTopic(adminClient: AdminClient, config: FasitProperties) =
        delete<DeleteTopic>(
                "a topic. Only members in KM-{topicName} are authorized".securityAndReponds(
                        BasicAuthSecurity(),
                        ok<DeleteTopicModel>(),
                        failed<AnError>(),
                        unAuthorized<Unit>()
                )) { param ->
            respondSelectiveCatch {

                val currentUser = call.principal<UserIdPrincipal>()!!.name
                val topicName = param.topicName

                val logEntry = "Topic deletion request by $currentUser - $topicName"
                application.environment.log.info(logEntry)

                val authorized = LDAPGroup(config).use { ldap -> ldap.userIsManager(topicName, currentUser) }

                if (authorized) {

                    application.environment.log.info("$currentUser is manager of $topicName")

                    // delete ACLs
                    val acls = AclBindingFilter(
                            ResourcePatternFilter(ResourceType.TOPIC, topicName, PatternType.LITERAL),
                            AccessControlEntryFilter.ANY
                    )

                    application.environment.log.info("ACLs delete request: $acls")

                    val aclsResult = try {
                        adminClient.deleteAcls(mutableListOf(acls)).all().get()
                        application.environment.log.info("ACLs deleted - $acls")
                        "deleted $acls"
                    } catch (e: Exception) {
                        application.environment.log.error("$EXCEPTION ACLs delete request $acls - $e")
                        "failure for $acls deletion, $e"
                    }

                    // delete related kafka ldap groups
                    val groupsResult = LDAPGroup(config).use { ldap -> ldap.deleteKafkaGroups(topicName) }

                    // delete the topic itself
                    val topicResult = try {
                        adminClient.deleteTopics(listOf(topicName)).all().get()
                        application.environment.log.info("Topic deleted - $topicName")
                        "deleted topic $topicName"
                    } catch (e: Exception) {
                        application.environment.log.error("$EXCEPTION topic delete request $topicName - $e")
                        "failure for topic $topicName deletion, $e"
                    }
                    Pair(HttpStatusCode.OK, DeleteTopicModel(topicResult, groupsResult, aclsResult))
                } else {

                    application.environment.log.info("$currentUser is NOT manager of $topicName")

                    Pair(HttpStatusCode.Unauthorized, "")
                }
            }
        }

/**
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/admin/AdminClient.html#describeConfigs-java.util.Collection-
 */

@Group(swGroup)
@Location("$TOPICS/{topicName}")
data class GetTopicConfig(val topicName: String)

data class GetTopicConfigModel(val name: String, val config: List<ConfigEntry>)

fun Routing.getTopicConfig(adminClient: AdminClient) =
        get<GetTopicConfig>("a topic's configuration".responds(
                ok<GetTopicConfigModel>(),
                failed<AnError>())) { param ->
            respondCatch {

                val topicName = param.topicName

                // NB! AdminClient is listing a default config independent of topic exists or not, verify!
                val existingTopics = adminClient.listTopics().listings().get().map { it.name() }

                if (existingTopics.contains(topicName))
                    GetTopicConfigModel(
                            topicName,
                            adminClient.describeConfigs(
                                    mutableListOf(
                                            ConfigResource(
                                                    ConfigResource.Type.TOPIC,
                                                    topicName
                                            )
                                    )
                            ).all().get().values.first().entries().toList()
                    )
                else
                    throw Exception("failure, topic $topicName does not exist")
            }
        }

/**
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/admin/ConfigEntry.html
 * e.g. {"name": "retention.ms","value": "3600000"}
 */

// will be enhanced with suitable set of config entries
enum class AllowedConfigEntries(val entryName: String) {
    RETENTION_MS("retention.ms"),
    RETENTION_BYTES("retention.bytes"),
    CLEANUP_POLICY("cleanup.policy")
}

@Group(swGroup)
@Location("$TOPICS/{topicName}")
data class PutTopicConfigEntry(val topicName: String)
data class PutTopicConfigEntryBody(val configentry: AllowedConfigEntries, val value: String)

data class PutTopicConfigEntryModel(
    val name: String,
    val configentry: AllowedConfigEntries,
    val status: String
)

fun Routing.updateTopicConfig(adminClient: AdminClient, config: FasitProperties) =
        put<PutTopicConfigEntry, PutTopicConfigEntryBody>(
                "a configuration entry for a topic. Only members in KM-{topicName} are authorized".securityAndReponds(
                BasicAuthSecurity(),
                ok<PutTopicConfigEntryModel>(),
                failed<AnError>(),
                unAuthorized<Unit>()
        )) { param, body ->
            respondSelectiveCatch {

                val currentUser = call.principal<UserIdPrincipal>()!!.name
                val topicName = param.topicName

                val logEntry = "Topic config. update request by $currentUser - $topicName"
                application.environment.log.info(logEntry)

                val authorized = LDAPGroup(config).use { ldap -> ldap.userIsManager(topicName, currentUser) }

                if (authorized) {

                    application.environment.log.info("$currentUser is manager of $topicName")

                    val configEntry = ConfigEntry(body.configentry.entryName, body.value)

                    // check whether configEntry is allowed
                    if (!AllowedConfigEntries.values().map { it.entryName }.contains(configEntry.name()))
                        throw Exception("configEntry ${configEntry.name()} is not allowed to update automatically")

                    val configResource = ConfigResource(ConfigResource.Type.TOPIC, topicName)
                    val configReq = mapOf(configResource to Config(listOf(configEntry)))

                    application.environment.log.info("Update topic config request: $configReq")

                    // NB! .all is throwing error... Use of future for specific entry instead
                    val res = adminClient.alterConfigs(configReq)
                            // .all()
                            .values()[configResource]
                            ?.get()
                            ?: PutTopicConfigEntryModel(topicName, body.configentry, "updated with ${body.value}")

                    Pair(HttpStatusCode.OK, res)
                } else {
                    application.environment.log.info("$currentUser is NOT manager of $topicName")
                    Pair(HttpStatusCode.Unauthorized, "")
                }
            }
        }

/**
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/admin/AdminClient.html#describeAcls-org.apache.kafka.common.acl.AclBindingFilter-
 */

@Group(swGroup)
@Location("$TOPICS/{topicName}/acls")
data class GetTopicACL(val topicName: String)

data class GetTopicACLModel(val name: String, val acls: List<AclBinding>)

fun Routing.getTopicAcls(adminClient: AdminClient) =
        get<GetTopicACL>("a topic's access control lists".responds(ok<GetTopicACLModel>(), failed<AnError>())) { param ->
            respondCatch {

                val topicName = param.topicName

                GetTopicACLModel(
                        topicName,
                        adminClient.describeAcls(
                                AclBindingFilter(
                                        ResourcePatternFilter(ResourceType.TOPIC, topicName, PatternType.LITERAL),
                                        AccessControlEntryFilter.ANY)
                        )
                                .values()
                                .get().toList()
                )
            }
        }

/**
 * See LDAPGroup::getKafkaGroupsAndMembers
 */

@Group(swGroup)
@Location("$TOPICS/{topicName}/groups")
data class GetTopicGroups(val topicName: String)

data class GetTopicGroupsModel(val name: String, val groups: List<KafkaGroup>)

fun Routing.getTopicGroups(config: FasitProperties) =
        get<GetTopicGroups>("a topic's groups".responds(ok<GetTopicGroupsModel>(), failed<AnError>())) { param ->
            respondCatch {

                val topicName = param.topicName

                GetTopicGroupsModel(
                        topicName,
                        LDAPGroup(config).use { ldap -> ldap.getKafkaGroupsAndMembers(topicName) }
                )
            }
        }

/**
 * See LDAPGroup::updateKafkaGroupMembership
 */

@Group(swGroup)
@Location("$TOPICS/{topicName}/groups")
data class PutTopicGMember(val topicName: String)

data class PutTopicGMemberModel(
    val name: String,
    val updaterequest: UpdateKafkaGroupMember,
    val status: SLDAPResult
)

fun Routing.updateTopicGroup(config: FasitProperties) =
        put<PutTopicGMember, UpdateKafkaGroupMember>(
                "add/remove members in topic groups. Only members in KM-{topicName} are authorized ".securityAndReponds(
                BasicAuthSecurity(),
                ok<PutTopicGMemberModel>(),
                failed<AnError>(),
                unAuthorized<Unit>()
        )) { param, body ->
            respondSelectiveCatch {

                val currentUser = call.principal<UserIdPrincipal>()!!.name
                val topicName = param.topicName

                val logEntry = "Topic group membership update request by " +
                        "${this.context.authentication.principal} - $topicName "
                application.environment.log.info(logEntry)

                val authorized = LDAPGroup(config).use { ldap -> ldap.userIsManager(topicName, currentUser) }

                if (authorized) {

                    application.environment.log.info("$currentUser is manager of $topicName")

                    HttpStatusCode.OK to LDAPGroup(config)
                            .use { PutTopicGMemberModel(topicName, body, it.updateKafkaGroupMembership(topicName, body)) }
                            .also { application.environment.log.info("$topicName's group has been updated") }
                } else {
                    application.environment.log.info("$currentUser is NOT manager of $topicName")
                    Pair(HttpStatusCode.Unauthorized, "")
                }
            }
        }

package no.nav.integrasjon.api.v1

import com.unboundid.ldap.sdk.ResultCode
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig
import io.ktor.application.ApplicationCall
import io.ktor.application.application
import io.ktor.application.call
import io.ktor.auth.UserIdPrincipal
import io.ktor.auth.authentication
import io.ktor.auth.principal
import io.ktor.http.HttpStatusCode
import io.ktor.locations.Location
import io.ktor.response.respond
import io.ktor.routing.Routing
import io.ktor.util.pipeline.PipelineContext
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import no.nav.integrasjon.EXCEPTION
import no.nav.integrasjon.Environment
import no.nav.integrasjon.api.nais.client.SERVICES_ERR_G
import no.nav.integrasjon.api.nais.client.SERVICES_ERR_K
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.BasicAuthSecurity
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.Group
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.badRequest
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.delete
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
import org.apache.kafka.clients.admin.AlterConfigOp
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.acl.AccessControlEntryFilter
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.acl.AclBindingFilter
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.ResourcePatternFilter
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
fun Routing.topicsAPI(adminClient: AdminClient?, environment: Environment) {

    getTopics(adminClient, environment)
    createNewTopic(adminClient, environment)

    deleteTopic(adminClient, environment)

    getTopicConfig(adminClient, environment)
    updateTopicConfig(adminClient, environment)

    getTopicAcls(adminClient, environment)

    getTopicGroups(environment)
    updateTopicGroup(environment)
}

private const val swGroup = "Topics"

/**
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/admin/AdminClient.html#listTopics-org.apache.kafka.clients.admin.ListTopicsOptions-
 */

@Group(swGroup)
@Location(TOPICS)
class GetTopics

data class GetTopicsModel(val topics: List<String>)

fun Routing.getTopics(adminClient: AdminClient?, environment: Environment) =
    get<GetTopics>("all topics".responds(ok<GetTopicsModel>(), serviceUnavailable<AnError>())) {
        respondOrServiceUnavailable {

            val topics = adminClient
                ?.listTopics()
                ?.names()
                ?.get(environment.kafka.kafkaTimeout, TimeUnit.MILLISECONDS)
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
fun getDefaultReplicationFactor(adminClient: AdminClient?, environment: Environment): Short =
    adminClient?.let { ac ->
        ac.describeCluster()
            .nodes()
            .get(environment.kafka.kafkaTimeout, TimeUnit.MILLISECONDS)
            .first().let { node ->
                ac.describeConfigs(
                    listOf(
                        ConfigResource(
                            ConfigResource.Type.BROKER,
                            node.idString()
                        )
                    )
                )
                    .all()
                    .get(environment.kafka.kafkaTimeout, TimeUnit.MILLISECONDS)
                    .values.first()
                    .get("default.replication.factor")
                    .value()
                    .toShort()
            }
    } ?: throw Exception(SERVICES_ERR_K)

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

fun Routing.createNewTopic(adminClient: AdminClient?, environment: Environment) =
    post<PostTopic, PostTopicBody>(
        "new topic. The authenticated user becomes member of KM-{newTopic}. Please add other team members"
            .securityAndReponds(
                BasicAuthSecurity(),
                ok<PostTopicModel>(),
                serviceUnavailable<AnError>(),
                badRequest<AnError>(),
                unAuthorized<Unit>()
            )
    ) { _, body ->

        val currentUser = call.principal<UserIdPrincipal>()!!.name
        val logEntry = "Topic creation request by $currentUser - $body"
        application.environment.log.info(logEntry)

        /**
         * Rule 1 - user must exist in current ldap environment in order to be part of group KM-<topic>
         */

        val userExist = try {
            LDAPGroup(environment).use { ldap -> ldap.userExists(currentUser) }
        } catch (e: Exception) {
            false
        }

        if (!userExist) {
            val msg = "authenticated user $currentUser doesn't exist as NAV ident or " +
                "service user in current LDAP domain, or ldap unreachable, cannot be manager of topic"
            application.environment.log.warn(msg)
            call.respond(HttpStatusCode.ServiceUnavailable, AnError(msg))
            return@post
        }

        /**
         * Rule 2 - must be a valid topic name, limited by group name length in LDAP and type of characters
         */

        if (!body.name.isValidTopicName()) {
            call.respond(
                HttpStatusCode.BadRequest, AnError(
                    "Invalid topic name - $body.name. " +
                        "Must contain [a..z]||[A..Z]||[0..9]||'-' only " +
                        "&& + length ≤ ${LDAPGroup.maxTopicNameLength()}"
                )
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
        val defaultRepFactor = try {
            getDefaultReplicationFactor(adminClient, environment)
        } catch (e: Exception) {
            repFactorError
        }

        if (defaultRepFactor == repFactorError) {
            call.respond(
                HttpStatusCode.ServiceUnavailable, AnError(
                    "Could not get replicationFactor for topic from Kafka"
                )
            )
            return@post
        }

        val newTopic = NewTopic(body.name, body.numPartitions, defaultRepFactor)

        // ensure groups are created before topic to prevent orphaned topics without managers in case LDAP operations fail
        val groupsResult = LDAPGroup(environment).use { ldap -> ldap.createKafkaGroups(newTopic.name(), currentUser) }
        val groupsAreOk = groupsResult
            .asSequence()
            .map { it.ldapResult.resultCode }
            .all { it == ResultCode.SUCCESS }
        if (groupsAreOk) {
            application.environment.log.info("Groups for topic $newTopic have been created")
        } else {
            application.environment.log.error("Groups for topic $newTopic have some issues")
            call.respond(
                HttpStatusCode.ServiceUnavailable, AnError(
                    "Could not create LDAP groups for topic - ${groupsResult.map { it.ldapResult.message }}"
                )
            )
            return@post
        }

        // create topic
        val (topicIsOk, topicResult) = try {
            adminClient?.let { ac ->
                ac.createTopics(mutableListOf(newTopic)).all()
                    .get(environment.kafka.kafkaTimeout, TimeUnit.MILLISECONDS)
                application.environment.log.info("Topic created - $newTopic")
                Pair(true, "created topic $newTopic")
            } ?: Pair(false, "failure for topic $newTopic creation, $SERVICES_ERR_K")
        } catch (e: Exception) {
            // TODO should have warning for topcis already exists
            application.environment.log.error("$EXCEPTION topic create request $newTopic - $e")
            call.respond(
                HttpStatusCode.ServiceUnavailable, AnError(
                    "Failed to create Kafka topic $newTopic - $e"
                )
            )
            return@post
        }

        // create ACLs based on kafka groups in LDAP, except manager group KM-
        val acls = groupsResult.asSequence()
            .filter { it.type != KafkaGroupType.MANAGER }
            .map { kafkaGroup -> kafkaGroup.type.intoAcls(newTopic.name()) }
            .flatten()

        application.environment.log.info("ACLs create request: $acls")

        val (aclsAreOk, aclsResult) = try {
            adminClient?.let { ac ->
                ac.createAcls(acls.toList()).all().get(environment.kafka.kafkaTimeout, TimeUnit.MILLISECONDS)
                application.environment.log.info("ACLs created - $acls")
                Pair(true, "created $acls")
            } ?: Pair(false, "failure for $acls creation, $SERVICES_ERR_K")
        } catch (e: Exception) {
            application.environment.log.error("$EXCEPTION ACLs create request $acls - $e")
            call.respond(
                HttpStatusCode.ServiceUnavailable, AnError(
                    "Failed to create ACLs $acls for Kafka topic - $e"
                )
            )
            return@post
        }

        val errorMsg = "Topic: $topicResult " +
            "Groups: ${groupsResult.map { it.ldapResult.message }} " +
            "ACLs: $aclsResult"

        when (topicIsOk && groupsAreOk && aclsAreOk) {
            true -> call.respond(PostTopicModel(topicResult, groupsResult, aclsResult))
            false -> call.respond(HttpStatusCode.ServiceUnavailable, AnError(errorMsg))
        }
    }

private enum class UserIsManager { LDAP_NOT_AVAILABLE, IS_NOT_MANAGER, IS_MANAGER, LDAP_NO_GROUPS_FOUND }

// prevent deletion of orphaned topics for any topic containing these terms
private val protectedOrphantedTopics: Set<String> = setOf(
    Topic.GROUP_METADATA_TOPIC_NAME,
    Topic.TRANSACTION_STATE_TOPIC_NAME,
    SchemaRegistryConfig.DEFAULT_KAFKASTORE_TOPIC,
    "-repartition",
    "-changelog",
    "-topic",
    "KTABLE-",
    "KSTREAM-"
)

private suspend fun PipelineContext<Unit, ApplicationCall>.userTopicManagerStatus(
    user: String,
    topicName: String,
    environment: Environment
): UserIsManager {

    val (isMngRequestOk, authorized) = try {
        Pair(true, LDAPGroup(environment).use { ldap -> ldap.userIsManager(topicName, user) })
    } catch (e: Exception) {
        application.environment.log.warn("Error when checking manager status for user $user for topic $topicName", e)
        Pair(first = false, second = false)
    }

    val groupsExist: Boolean = try {
        LDAPGroup(environment)
            .use { ldap -> ldap.getKafkaGroupsAndMembers(topicName) }
            .none { kafkaGroup -> kafkaGroup.ldapResult.resultCode == ResultCode.NO_SUCH_OBJECT }
    } catch (e: Exception) {
        application.environment.log.warn(SERVICES_ERR_G, e)
        call.respond(HttpStatusCode.ServiceUnavailable, AnError(SERVICES_ERR_G))
        return UserIsManager.LDAP_NOT_AVAILABLE
    }

    if (!groupsExist) {
        application.environment.log.warn("Groups not found for topic $topicName - probably orphaned")
        val isProtectedTopic = protectedOrphantedTopics.any { protectedTopicName ->
            topicName.contains(protectedTopicName, ignoreCase = true)
        }
        if (isProtectedTopic) {
            application.environment.log.warn("User attempted to delete internal protected topic, denying request")
            call.respond(HttpStatusCode.Unauthorized, AnError("Cannot delete internal topic"))
            return UserIsManager.IS_NOT_MANAGER
        } else {
            return UserIsManager.LDAP_NO_GROUPS_FOUND
        }
    }

    if (!isMngRequestOk) {
        application.environment.log.warn(SERVICES_ERR_G)
        call.respond(HttpStatusCode.ServiceUnavailable, AnError(SERVICES_ERR_G))
        return UserIsManager.LDAP_NOT_AVAILABLE
    }

    if (!authorized) {
        val msg = "$user is NOT manager of $topicName"
        application.environment.log.warn(msg)
        call.respond(HttpStatusCode.Unauthorized, AnError(msg))
        return UserIsManager.IS_NOT_MANAGER
    }

    application.environment.log.info("$user is manager of $topicName")

    return UserIsManager.IS_MANAGER
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

fun Routing.deleteTopic(adminClient: AdminClient?, environment: Environment) =
    delete<DeleteTopic>(
        "a topic. Only members in KM-{topicName} are authorized".securityAndReponds(
            BasicAuthSecurity(),
            ok<DeleteTopicModel>(),
            serviceUnavailable<AnError>(),
            badRequest<AnError>(),
            unAuthorized<AnError>()
        )
    ) { param ->

        val currentUser = call.principal<UserIdPrincipal>()!!.name
        val topicName = param.topicName

        val logEntry = "Topic deletion request by $currentUser - $topicName"
        application.environment.log.info(logEntry)

        val userIsManagerStatus = userTopicManagerStatus(currentUser, topicName, environment)
        when (userIsManagerStatus) {
            UserIsManager.LDAP_NOT_AVAILABLE -> return@delete
            UserIsManager.IS_NOT_MANAGER -> return@delete
            UserIsManager.IS_MANAGER -> {
            }
            UserIsManager.LDAP_NO_GROUPS_FOUND -> {
                application.environment.log.warn("No groups found - assuming orphaned topic and allowing delete")
            }
        }
        // delete ACLs
        val acls = AclBindingFilter(
            ResourcePatternFilter(ResourceType.TOPIC, topicName, PatternType.LITERAL),
            AccessControlEntryFilter.ANY
        )

        application.environment.log.info("ACLs delete request: $acls")

        val (aclsAreOk: Boolean, aclsResult: String) = if (userIsManagerStatus != UserIsManager.LDAP_NO_GROUPS_FOUND) {
            try {
                adminClient?.let { ac ->
                    ac.deleteAcls(mutableListOf(acls)).all().get(environment.kafka.kafkaTimeout, TimeUnit.MILLISECONDS)
                    application.environment.log.info("ACLs deleted - $acls")
                    Pair(true, "deleted $acls")
                } ?: Pair(false, "failure for $acls deletion, $SERVICES_ERR_K")
            } catch (e: Exception) {
                application.environment.log.error("$EXCEPTION ACLs delete request $acls - $e")
                Pair(false, "failure for $acls deletion, $e")
            }
        } else {
            true to "no acls to delete"
        }

        // delete related kafka ldap groups
        val groupsResult: List<KafkaGroup> = if (userIsManagerStatus != UserIsManager.LDAP_NO_GROUPS_FOUND) {
            LDAPGroup(environment).use { ldap -> ldap.deleteKafkaGroups(topicName) }
        } else {
            emptyList()
        }
        val groupsAreOk: Boolean = groupsResult
            .asSequence()
            .map { it.ldapResult.resultCode }
            .all { it == ResultCode.SUCCESS }

        // delete the topic itself
        val (topicIsOk, topicResult) = try {
            adminClient?.let { ac ->
                ac.deleteTopics(listOf(topicName)).all().get(environment.kafka.kafkaTimeout, TimeUnit.MILLISECONDS)
                application.environment.log.info("Topic deleted - $topicName")
                Pair(true, "deleted topic $topicName")
            } ?: Pair(false, "failure for topic $topicName deletion, $SERVICES_ERR_K")
        } catch (e: Exception) {
            application.environment.log.error("$EXCEPTION topic delete request $topicName - $e")
            Pair(false, "failure for topic $topicName deletion, $e")
        }

        val errorMsg = "Topic: $topicResult " +
            "Groups: ${groupsResult.map { it.ldapResult.message }} " +
            "ACLs: $aclsResult"

        when (topicIsOk && groupsAreOk && aclsAreOk) {
            true -> call.respond(DeleteTopicModel(topicResult, groupsResult, aclsResult))
            false -> call.respond(HttpStatusCode.ServiceUnavailable, AnError(errorMsg))
        }
    }

/**
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/admin/AdminClient.html#describeConfigs-java.util.Collection-
 */

@Group(swGroup)
@Location("$TOPICS/{topicName}")
data class GetTopicConfig(val topicName: String)

data class GetTopicConfigModel(val name: String, val config: List<ConfigEntry>)

fun Routing.getTopicConfig(adminClient: AdminClient?, environment: Environment) =
    get<GetTopicConfig>(
        "a topic's configuration".responds(
            ok<GetTopicConfigModel>(),
            serviceUnavailable<AnError>(),
            badRequest<AnError>()
        )
    ) { param ->

        val topicName = param.topicName

        // NB! AdminClient is listing a default config independent of topic exists or not, verify existence!
        val (topicsRequestOk, existingTopics) = fetchTopics(adminClient, environment, topicName)

        if (!topicsRequestOk) {
            call.respond(HttpStatusCode.ServiceUnavailable, AnError(SERVICES_ERR_K))
            return@get
        }

        if (existingTopics.isNotEmpty() && !existingTopics.contains(topicName)) {
            call.respond(HttpStatusCode.BadRequest, AnError("Cannot find topic $topicName"))
            return@get
        }

        val (topicConfigRequestOk, topicConfig) = fetchTopicConfig(adminClient, topicName, environment)

        if (!topicConfigRequestOk) {
            call.respond(HttpStatusCode.ServiceUnavailable, AnError(SERVICES_ERR_K))
            return@get
        }

        call.respond(GetTopicConfigModel(topicName, topicConfig))
    }

/**
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/admin/ConfigEntry.html
 * e.g. {"name": "retention.ms","value": "3600000"}
 */

// will be enhanced with suitable set of config entries
enum class AllowedConfigEntries(val entryName: String) {
    RETENTION_MS("retention.ms"),
    RETENTION_BYTES("retention.bytes"),
    CLEANUP_POLICY("cleanup.policy"),
    DELETE_RETENTION_MS("delete.retention.ms"),
    MIN_COMPACTION_LAG_MS("min.compaction.lag.ms")
}

@Group(swGroup)
@Location("$TOPICS/{topicName}")
data class PutTopicConfigEntry(val topicName: String)

data class ConfigEntries(
    val entries: List<PutTopicConfigEntryBody>
)

data class PutTopicConfigEntryBody(
    val configentry: AllowedConfigEntries,
    val value: String
)

data class PutTopicConfigEntryModel(
    val name: String,
    val configentry: List<AllowedConfigEntries>,
    val status: String
)

fun Routing.updateTopicConfig(adminClient: AdminClient?, environment: Environment) =
    put<PutTopicConfigEntry, ConfigEntries>(
        "a configuration entry for a topic. Only members in KM-{topicName} are authorized".securityAndReponds(
            BasicAuthSecurity(),
            ok<PutTopicConfigEntryModel>(),
            serviceUnavailable<AnError>(),
            badRequest<AnError>(),
            unAuthorized<AnError>()
        )
    ) { param, body ->

        val currentUser = call.principal<UserIdPrincipal>()!!.name
        val topicName = param.topicName

        val logEntry = "Topic config. update request by $currentUser - $topicName"
        application.environment.log.info(logEntry)

        when (userTopicManagerStatus(currentUser, topicName, environment)) {
            UserIsManager.LDAP_NOT_AVAILABLE -> return@put
            UserIsManager.IS_NOT_MANAGER -> return@put
            UserIsManager.LDAP_NO_GROUPS_FOUND -> {
                call.respond(
                    HttpStatusCode.Unauthorized,
                    AnError("No groups found for topic - delete the topic and recreate it")
                )
                return@put
            }
            UserIsManager.IS_MANAGER -> {
            }
        }

        val newConfigEntries = try {
            body.entries.map {
                ConfigEntry(it.configentry.entryName, it.value.toLowerCase())
            }
        } catch (e: Exception) {
            log.warn("Could not parse input body", e)
            null
        }

        if (newConfigEntries == null) {
            val msg = "Not supported configEntry, please see swagger documentation and model"
            application.environment.log.error(msg)
            call.respond(HttpStatusCode.BadRequest, AnError(msg))
            return@put
        }

        newConfigEntries.map { entry ->
            if (!AllowedConfigEntries.values().map { it.entryName }.contains(entry.name())) {
                val msg = "configEntry ${entry.name()} is not allowed to update automatically"
                application.environment.log.error(msg)
                call.respond(HttpStatusCode.BadRequest, AnError(msg))
                return@put
            }
        }

        val (topicsRequestOk, existingTopics) = fetchTopics(adminClient, environment, topicName)

        if (!topicsRequestOk) {
            call.respond(HttpStatusCode.ServiceUnavailable, AnError(SERVICES_ERR_K))
            return@put
        }

        if (existingTopics.isNotEmpty() && !existingTopics.contains(topicName)) {
            call.respond(HttpStatusCode.BadRequest, AnError("Cannot find topic $topicName"))
            return@put
        }

        // Use existing topic configuration and only update entries provided in request
        val updatedTopicConfig: List<AlterConfigOp> = newConfigEntries
            .map { entry -> AlterConfigOp(entry, AlterConfigOp.OpType.SET) }
        val configResource = ConfigResource(ConfigResource.Type.TOPIC, topicName)
        val configReq: Map<ConfigResource, Collection<AlterConfigOp>> = mapOf(configResource to updatedTopicConfig)
        application.environment.log.info("Update topic config request: $configReq")

        // NB! .all is throwing error... Use of future for specific entry instead
        val (alterConfigRequestOk, _) = try {
            Pair(true, adminClient?.let { ac ->
                ac.incrementalAlterConfigs(configReq)
                    .values()
                    .get(configResource)
                    ?.get(environment.kafka.kafkaTimeout, TimeUnit.MILLISECONDS) ?: Unit
            } ?: throw Exception(SERVICES_ERR_K)
            )
        } catch (e: Exception) {
            Pair(false, Unit)
        }

        if (!alterConfigRequestOk) {
            application.environment.log.error(SERVICES_ERR_K)
            call.respond(HttpStatusCode.ServiceUnavailable, AnError(SERVICES_ERR_K))
            return@put
        }

        call.respond(
            PutTopicConfigEntryModel(
                topicName,
                body.entries.map { it.configentry },
                "updated with ${body.entries.map { it.value }}"
            )
        )
    }

/**
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/admin/AdminClient.html#describeAcls-org.apache.kafka.common.acl.AclBindingFilter-
 */

@Group(swGroup)
@Location("$TOPICS/{topicName}/acls")
data class GetTopicACL(val topicName: String)

data class GetTopicACLModel(val name: String, val acls: List<AclBinding>)

fun Routing.getTopicAcls(adminClient: AdminClient?, environment: Environment) =
    get<GetTopicACL>(
        "a topic's access control lists".responds(
            ok<GetTopicACLModel>(),
            serviceUnavailable<AnError>()
        )
    ) { param ->

        val topicName = param.topicName

        val aclFilter = AclBindingFilter(
            ResourcePatternFilter(ResourceType.TOPIC, topicName, PatternType.LITERAL),
            AccessControlEntryFilter.ANY
        )

        val (aclRequestOk, acls) = try {
            Pair(
                true, adminClient
                    ?.describeAcls(aclFilter)
                    ?.values()
                    ?.get(environment.kafka.kafkaTimeout, TimeUnit.MILLISECONDS)
                    ?.toList()
                    ?: throw Exception(SERVICES_ERR_K)
            )
        } catch (e: Exception) {
            Pair(false, emptyList<AclBinding>())
        }

        if (!aclRequestOk) {
            application.environment.log.error(SERVICES_ERR_K)
            call.respond(HttpStatusCode.ServiceUnavailable, AnError(SERVICES_ERR_K))
            return@get
        }

        call.respond(GetTopicACLModel(topicName, acls))
    }

/**
 * See LDAPGroup::getKafkaGroupsAndMembers
 */

@Group(swGroup)
@Location("$TOPICS/{topicName}/groups")
data class GetTopicGroups(val topicName: String)

data class GetTopicGroupsModel(val name: String, val groups: List<KafkaGroup>)

fun Routing.getTopicGroups(environment: Environment) =
    get<GetTopicGroups>(
        "a topic's groups".responds(
            ok<GetTopicGroupsModel>(),
            serviceUnavailable<AnError>()
        )
    ) { param ->
        respondOrServiceUnavailable(environment) { lc ->

            val topicName = param.topicName
            GetTopicGroupsModel(topicName, lc.getKafkaGroupsAndMembers(topicName))
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

fun Routing.updateTopicGroup(environment: Environment) =
    put<PutTopicGMember, UpdateKafkaGroupMember>(
        "add/remove members in topic groups. Only members in KM-{topicName} are authorized ".securityAndReponds(
            BasicAuthSecurity(),
            ok<PutTopicGMemberModel>(),
            serviceUnavailable<AnError>(),
            badRequest<AnError>(),
            unAuthorized<AnError>()
        )
    ) { param, body ->

        val currentUser = call.principal<UserIdPrincipal>()!!.name
        val topicName = param.topicName

        val logEntry = "Topic group membership update request by " +
            "${this.context.authentication.principal} - $topicName "
        application.environment.log.info(logEntry)

        when (userTopicManagerStatus(currentUser, topicName, environment)) {
            UserIsManager.LDAP_NOT_AVAILABLE -> return@put
            UserIsManager.IS_NOT_MANAGER -> return@put
            UserIsManager.LDAP_NO_GROUPS_FOUND -> {
                call.respond(
                    HttpStatusCode.Unauthorized,
                    AnError("No groups found for topic - delete the topic and recreate it")
                )
                return@put
            }
            UserIsManager.IS_MANAGER -> {
            }
        }

        val (updateRequestOk, result) = try {
            Pair(true, LDAPGroup(environment).use { lc -> lc.updateKafkaGroupMembership(topicName, body) })
        } catch (e: Exception) {
            Pair(false, SLDAPResult())
        }

        // TODO 1) User not found 2) user account not allowed in KP and KC groups

        if (!updateRequestOk) {
            val msg = "User not found, user not allowed in KP and KC, exception..."
            application.environment.log.error(msg)
            call.respond(HttpStatusCode.ServiceUnavailable, AnError(msg))
            return@put
        }

        application.environment.log.info("$topicName's group has been updated")
        call.respond(PutTopicGMemberModel(topicName, body, result))
    }

private fun PipelineContext<Unit, ApplicationCall>.fetchTopics(
    adminClient: AdminClient?,
    environment: Environment,
    topicName: String
): Pair<Boolean, List<String>> {
    return try {
        Pair(true, adminClient?.let { ac ->
            ac.listTopics()
                .listings()
                .get(environment.kafka.kafkaTimeout, TimeUnit.MILLISECONDS)
                .map { it.name() }
        } ?: throw Exception(SERVICES_ERR_K)
        )
    } catch (e: Exception) {
        application.environment.log.error("$EXCEPTION topic get config request $topicName - $e")
        Pair(false, emptyList())
    }
}

private fun PipelineContext<Unit, ApplicationCall>.fetchTopicConfig(
    adminClient: AdminClient?,
    topicName: String,
    environment: Environment
): Pair<Boolean, List<ConfigEntry>> {
    return try {
        Pair(
            true,
            adminClient
                ?.describeConfigs(mutableListOf(ConfigResource(ConfigResource.Type.TOPIC, topicName)))
                ?.all()
                ?.get(environment.kafka.kafkaTimeout, TimeUnit.MILLISECONDS)
                ?.values
                ?.first()
                ?.entries()
                ?.toList()
                ?: throw Exception(SERVICES_ERR_K)
        )
    } catch (e: Exception) {
        application.environment.log.error("$EXCEPTION topic get config request $topicName - $e")
        Pair(false, emptyList())
    }
}

package no.nav.integrasjon.api.v1

import com.unboundid.ldap.sdk.ResultCode
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
import no.nav.integrasjon.FasitProperties
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
fun Routing.topicsAPI(adminClient: AdminClient?, fasitConfig: FasitProperties) {

    getTopics(adminClient, fasitConfig)
    createNewTopic(adminClient, fasitConfig)

    deleteTopic(adminClient, fasitConfig)

    getTopicConfig(adminClient, fasitConfig)
    updateTopicConfig(adminClient, fasitConfig)

    getTopicAcls(adminClient, fasitConfig)

    getTopicGroups(fasitConfig)
    updateTopicGroup(fasitConfig)
}

private const val swGroup = "Topics"

/**
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/admin/AdminClient.html#listTopics-org.apache.kafka.clients.admin.ListTopicsOptions-
 */

@Group(swGroup)
@Location(TOPICS)
class GetTopics

data class GetTopicsModel(val topics: List<String>)

fun Routing.getTopics(adminClient: AdminClient?, fasitConfig: FasitProperties) =
    get<GetTopics>("all topics".responds(ok<GetTopicsModel>(), serviceUnavailable<AnError>())) {
        respondOrServiceUnavailable {

            val topics = adminClient
                ?.listTopics()
                ?.names()
                ?.get(fasitConfig.kafkaTimeout, TimeUnit.MILLISECONDS)
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
fun getDefaultReplicationFactor(adminClient: AdminClient?, fasitConfig: FasitProperties): Short =
    adminClient?.let { ac ->
        ac.describeCluster()
            .nodes()
            .get(fasitConfig.kafkaTimeout, TimeUnit.MILLISECONDS)
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
                    .get(fasitConfig.kafkaTimeout, TimeUnit.MILLISECONDS)
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

fun Routing.createNewTopic(adminClient: AdminClient?, fasitConfig: FasitProperties) =
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
            LDAPGroup(fasitConfig).use { ldap -> ldap.userExists(currentUser) }
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
            getDefaultReplicationFactor(adminClient, fasitConfig)
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

        val (topicIsOk, topicResult) = try {
            adminClient?.let { ac ->
                ac.createTopics(mutableListOf(newTopic)).all().get(fasitConfig.kafkaTimeout, TimeUnit.MILLISECONDS)
                application.environment.log.info("Topic created - $newTopic")
                Pair(true, "created topic $newTopic")
            } ?: Pair(false, "failure for topic $newTopic creation, $SERVICES_ERR_K")
        } catch (e: Exception) {
            // TODO should have warning for topcis already exists
            application.environment.log.error("$EXCEPTION topic create request $newTopic - $e")
            Pair(false, "failure for topic $newTopic creation, $e")
        }

        val groupsResult = LDAPGroup(fasitConfig).use { ldap -> ldap.createKafkaGroups(newTopic.name(), currentUser) }

        val groupsAreOk = groupsResult
            .asSequence()
            .map { it.ldapResult.resultCode }
            .all { it == ResultCode.SUCCESS }

        if (groupsAreOk)
            application.environment.log.info("Groups for topic $newTopic have been created")
        else
            application.environment.log.info("Groups for topic $newTopic have some issues")

        // create ACLs based on kafka groups in LDAP, except manager group KM-
        val acls = groupsResult.asSequence()
            .filter { it.type != KafkaGroupType.MANAGER }
            .map { kafkaGroup -> kafkaGroup.type.intoAcls(newTopic.name()) }
            .flatten()

        application.environment.log.info("ACLs create request: $acls")

        val (aclsAreOk, aclsResult) = try {
            adminClient?.let { ac ->
                ac.createAcls(acls.toList()).all().get(fasitConfig.kafkaTimeout, TimeUnit.MILLISECONDS)
                application.environment.log.info("ACLs created - $acls")
                Pair(true, "created $acls")
            } ?: Pair(false, "failure for $acls creation, $SERVICES_ERR_K")
        } catch (e: Exception) {
            application.environment.log.error("$EXCEPTION ACLs create request $acls - $e")
            Pair(false, "failure for $acls creation, $e")
        }

        val errorMsg = "Topic: $topicResult " +
            "Groups: ${groupsResult.map { it.ldapResult.message }} " +
            "ACLs: $aclsResult"

        when (topicIsOk && groupsAreOk && aclsAreOk) {
            true -> call.respond(PostTopicModel(topicResult, groupsResult, aclsResult))
            false -> call.respond(HttpStatusCode.ServiceUnavailable, AnError(errorMsg))
        }
    }

private enum class UserIsManager { LDAP_NOT_AVAILABLE, IS_NOT_MANAGER, IS_MANAGER }

private suspend fun PipelineContext<Unit, ApplicationCall>.userTopicManagerStatus(
    user: String,
    topicName: String,
    fasitConfig: FasitProperties
): UserIsManager {

    val (isMngRequestOk, authorized) = try {
        Pair(true, LDAPGroup(fasitConfig).use { ldap -> ldap.userIsManager(topicName, user) })
    } catch (e: Exception) {
        Pair(first = false, second = false)
    }

    if (!isMngRequestOk) {
        application.environment.log.warn(SERVICES_ERR_G)
        call.respond(HttpStatusCode.ServiceUnavailable, AnError(SERVICES_ERR_G))
        return UserIsManager.LDAP_NOT_AVAILABLE
    }

    if (!authorized) {
        val msg = "$user is NOT manager of $topicName"
        application.environment.log.warn(msg)
        call.respond(HttpStatusCode.BadRequest, AnError(msg))
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

fun Routing.deleteTopic(adminClient: AdminClient?, fasitConfig: FasitProperties) =
    delete<DeleteTopic>(
        "a topic. Only members in KM-{topicName} are authorized".securityAndReponds(
            BasicAuthSecurity(),
            ok<DeleteTopicModel>(),
            serviceUnavailable<AnError>(),
            badRequest<AnError>(),
            unAuthorized<Unit>()
        )
    ) { param ->

        val currentUser = call.principal<UserIdPrincipal>()!!.name
        val topicName = param.topicName

        val logEntry = "Topic deletion request by $currentUser - $topicName"
        application.environment.log.info(logEntry)

        when (userTopicManagerStatus(currentUser, topicName, fasitConfig)) {
            UserIsManager.LDAP_NOT_AVAILABLE -> return@delete
            UserIsManager.IS_NOT_MANAGER -> return@delete
            else -> {
            }
        }

        // delete ACLs
        val acls = AclBindingFilter(
            ResourcePatternFilter(ResourceType.TOPIC, topicName, PatternType.LITERAL),
            AccessControlEntryFilter.ANY
        )

        application.environment.log.info("ACLs delete request: $acls")

        val (aclsAreOk, aclsResult) = try {
            adminClient?.let { ac ->
                ac.deleteAcls(mutableListOf(acls)).all().get(fasitConfig.kafkaTimeout, TimeUnit.MILLISECONDS)
                application.environment.log.info("ACLs deleted - $acls")
                Pair(true, "deleted $acls")
            } ?: Pair(false, "failure for $acls deletion, $SERVICES_ERR_K")
        } catch (e: Exception) {
            application.environment.log.error("$EXCEPTION ACLs delete request $acls - $e")
            Pair(false, "failure for $acls deletion, $e")
        }

        // delete related kafka ldap groups
        val groupsResult = LDAPGroup(fasitConfig).use { ldap -> ldap.deleteKafkaGroups(topicName) }
        val groupsAreOk = groupsResult
            .asSequence()
            .map { it.ldapResult.resultCode }
            .all { it == ResultCode.SUCCESS }

        // delete the topic itself
        val (topicIsOk, topicResult) = try {
            adminClient?.let { ac ->
                ac.deleteTopics(listOf(topicName)).all().get(fasitConfig.kafkaTimeout, TimeUnit.MILLISECONDS)
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

fun Routing.getTopicConfig(adminClient: AdminClient?, fasitConfig: FasitProperties) =
    get<GetTopicConfig>(
        "a topic's configuration".responds(
            ok<GetTopicConfigModel>(),
            serviceUnavailable<AnError>(),
            badRequest<AnError>()
        )
    ) { param ->

        val topicName = param.topicName

        // NB! AdminClient is listing a default config independent of topic exists or not, verify existence!
        val (topicsRequestOk, existingTopics) = fetchTopics(adminClient, fasitConfig, topicName)

        if (!topicsRequestOk) {
            call.respond(HttpStatusCode.ServiceUnavailable, AnError(SERVICES_ERR_K))
            return@get
        }

        if (existingTopics.isNotEmpty() && !existingTopics.contains(topicName)) {
            call.respond(HttpStatusCode.BadRequest, AnError("Cannot find topic $topicName"))
            return@get
        }

        val (topicConfigRequestOk, topicConfig) = fetchTopicConfig(adminClient, topicName, fasitConfig)

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
    DELETE_RETENTION_MS("delete.retention.ms")
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

fun Routing.updateTopicConfig(adminClient: AdminClient?, fasitConfig: FasitProperties) =
    put<PutTopicConfigEntry, ConfigEntries>(
        "a configuration entry for a topic. Only members in KM-{topicName} are authorized".securityAndReponds(
            BasicAuthSecurity(),
            ok<PutTopicConfigEntryModel>(),
            serviceUnavailable<AnError>(),
            badRequest<AnError>(),
            unAuthorized<Unit>()
        )
    ) { param, body ->

        val currentUser = call.principal<UserIdPrincipal>()!!.name
        val topicName = param.topicName

        val logEntry = "Topic config. update request by $currentUser - $topicName"
        application.environment.log.info(logEntry)

        when (userTopicManagerStatus(currentUser, topicName, fasitConfig)) {
            UserIsManager.LDAP_NOT_AVAILABLE -> return@put
            UserIsManager.IS_NOT_MANAGER -> return@put
            else -> {
            }
        }

        val newConfigEntries = try {
            body.entries.map {
                ConfigEntry(it.configentry.entryName, it.value)
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

        val (topicsRequestOk, existingTopics) = fetchTopics(adminClient, fasitConfig, topicName)

        if (!topicsRequestOk) {
            call.respond(HttpStatusCode.ServiceUnavailable, AnError(SERVICES_ERR_K))
            return@put
        }

        if (existingTopics.isNotEmpty() && !existingTopics.contains(topicName)) {
            call.respond(HttpStatusCode.BadRequest, AnError("Cannot find topic $topicName"))
            return@put
        }

        val (topicConfigRequestOk, existingTopicConfig) = fetchTopicConfig(adminClient, topicName, fasitConfig)

        if (!topicConfigRequestOk) {
            call.respond(HttpStatusCode.ServiceUnavailable, AnError(SERVICES_ERR_K))
            return@put
        }

        // Use existing topic configuration and only update entries provided in request
        val newConfigEntriesMap: Map<String, String> = newConfigEntries.associate { it.name() to it.value() }
        val updatedTopicConfig: List<ConfigEntry> = existingTopicConfig.map { existingEntry ->
            if (newConfigEntriesMap.containsKey(existingEntry.name())) {
                ConfigEntry(existingEntry.name(), newConfigEntriesMap[existingEntry.name()])
            } else {
                existingEntry
            }
        }

        val configResource = ConfigResource(ConfigResource.Type.TOPIC, topicName)
        val configReq = mapOf(configResource to Config(updatedTopicConfig))

        application.environment.log.info("Update topic config request: $configReq")

        // NB! .all is throwing error... Use of future for specific entry instead
        val (alterConfigRequestOk, _) = try {
            Pair(true, adminClient?.let { ac ->
                ac.alterConfigs(configReq)
                    .values()
                    .get(configResource)
                    ?.get(fasitConfig.kafkaTimeout, TimeUnit.MILLISECONDS) ?: Unit
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

fun Routing.getTopicAcls(adminClient: AdminClient?, fasitConfig: FasitProperties) =
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
                    ?.get(fasitConfig.kafkaTimeout, TimeUnit.MILLISECONDS)
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

fun Routing.getTopicGroups(fasitConfig: FasitProperties) =
    get<GetTopicGroups>(
        "a topic's groups".responds(
            ok<GetTopicGroupsModel>(),
            serviceUnavailable<AnError>()
        )
    ) { param ->
        respondOrServiceUnavailable(fasitConfig) { lc ->
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

fun Routing.updateTopicGroup(fasitConfig: FasitProperties) =
    put<PutTopicGMember, UpdateKafkaGroupMember>(
        "add/remove members in topic groups. Only members in KM-{topicName} are authorized ".securityAndReponds(
            BasicAuthSecurity(),
            ok<PutTopicGMemberModel>(),
            serviceUnavailable<AnError>(),
            badRequest<AnError>(),
            unAuthorized<Unit>()
        )
    ) { param, body ->

        val currentUser = call.principal<UserIdPrincipal>()!!.name
        val topicName = param.topicName

        val logEntry = "Topic group membership update request by " +
            "${this.context.authentication.principal} - $topicName "
        application.environment.log.info(logEntry)

        when (userTopicManagerStatus(currentUser, topicName, fasitConfig)) {
            UserIsManager.LDAP_NOT_AVAILABLE -> return@put
            UserIsManager.IS_NOT_MANAGER -> return@put
            else -> {
            }
        }

        val (updateRequestOk, result) = try {
            Pair(true, LDAPGroup(fasitConfig).use { lc -> lc.updateKafkaGroupMembership(topicName, body) })
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
    fasitConfig: FasitProperties,
    topicName: String
): Pair<Boolean, List<String>> {
    return try {
        Pair(true, adminClient?.let { ac ->
            ac.listTopics()
                .listings()
                .get(fasitConfig.kafkaTimeout, TimeUnit.MILLISECONDS)
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
    fasitConfig: FasitProperties
): Pair<Boolean, List<ConfigEntry>> {
    return try {
        Pair(
            true,
            adminClient
                ?.describeConfigs(mutableListOf(ConfigResource(ConfigResource.Type.TOPIC, topicName)))
                ?.all()
                ?.get(fasitConfig.kafkaTimeout, TimeUnit.MILLISECONDS)
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

package no.nav.integrasjon.api.v1

import io.ktor.application.application
import io.ktor.application.call
import io.ktor.auth.UserIdPrincipal
import io.ktor.auth.principal
import io.ktor.http.HttpStatusCode
import io.ktor.locations.Location
import io.ktor.response.respond
import io.ktor.routing.Routing
import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.integrasjon.EXCEPTION
import no.nav.integrasjon.Environment
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.BasicAuthSecurity
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.Group
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.badRequest
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.ok
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.put
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.securityAndReponds
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.serviceUnavailable
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.unAuthorized
import no.nav.integrasjon.ldap.KafkaGroupType
import no.nav.integrasjon.ldap.LDAPGroup
import no.nav.integrasjon.ldap.intoAcls
import no.nav.integrasjon.ldap.toGroupName
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AlterConfigOp
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.clients.admin.NewPartitions
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.config.ConfigResource
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.util.UUID
import java.util.concurrent.TimeUnit

val log: Logger = LoggerFactory.getLogger("kafka-adminrest.oneshot.v1")

data class OneshotCreationRequest(
    val topics: List<TopicCreation>
)

data class RoleMember(
    val member: String,
    val role: KafkaGroupType
)

data class TopicCreation(
    val topicName: String,
    val numPartitions: Int,
    val configEntries: Map<String, String>?,
    val members: List<RoleMember>
)

private data class TopicPartitions(
    val topicName: String,
    val existingCount: Int,
    val requestedCount: Int
)

enum class OneshotStatus {
    ERROR,
    OK
}

data class GroupMember(val group: String, val user: String)

data class OneshotResponse(
    val status: OneshotStatus,
    val message: String,
    val data: OneshotResult? = null,
    val requestId: String
)

data class OneshotResult(val creationId: String, val topics: List<TopicCreation>)

@Group("Oneshot")
@Location(ONESHOT)
class Oneshot

@Suppress("SENSELESS_COMPARISON")
fun Routing.registerOneshotApi(adminClient: AdminClient?, environment: Environment) {
    put<Oneshot, OneshotCreationRequest>(
        "Provides a one-shot kafka topic creation and ACL creation, it returns an ID and an endpoint where you can await a result from this request"
            .securityAndReponds(
                BasicAuthSecurity(),
                ok<OneshotResponse>(),
                badRequest<OneshotResponse>(),
                serviceUnavailable<OneshotResponse>(),
                unAuthorized<OneshotResponse>()
            )
    ) { _, request ->

        val currentUser = call.principal<UserIdPrincipal>()!!.name.lowercase()

        val uuid = try {
            MDC.get("callId") ?: UUID.randomUUID().toString()
        } catch (e: Exception) {
            UUID.randomUUID().toString()
        }

        val logKeys = arrayOf(
            keyValue("requestId", uuid),
            keyValue("currentUser", currentUser)
        )
        val logFormat = logKeys.joinToString(separator = ", ", prefix = "(", postfix = ")") { "{}" }

        log.info("Oneshot topic creation request initiated, request: $request $logFormat", *logKeys)

        LDAPGroup(environment).use { ldap ->
            if (!ldap.userExists(currentUser)) {
                val err = OneshotResponse(
                    status = OneshotStatus.ERROR,
                    message = "authenticated user $currentUser doesn't exist as NAV ident or service user in " +
                        "current LDAP domain, cannot be manager of topic",
                    requestId = uuid
                )
                application.environment.log.error(EXCEPTION, err)
                call.respond(HttpStatusCode.Unauthorized, err)
                return@put
            }

            log.info("Validating config entries$logFormat", *logKeys)
            request.topics.flatMap { it.configEntries?.entries ?: setOf() }.filter { entry ->
                AllowedConfigEntries.values().none { it.entryName == entry.key }
            }.any {
                val err = OneshotResponse(
                    status = OneshotStatus.ERROR,
                    message = "configEntry ${it.key} is not allowed to update automatically",
                    requestId = uuid
                )
                call.respond(HttpStatusCode.BadRequest, err)
                return@put
            }

            // Fetch existing topics
            val existingTopics = try {
                adminClient?.listTopics()?.listings()?.get(environment.kafka.kafkaTimeout, TimeUnit.MILLISECONDS)?.map { it.name() }
                    ?: emptyList()
            } catch (e: Exception) {
                log.error("Exception caught while getting existing topic(s) $logFormat", logKeys, e)
                call.respond(
                    HttpStatusCode.ServiceUnavailable,
                    OneshotResponse(
                        status = OneshotStatus.ERROR,
                        message = "Failed to get topic from kafka",
                        requestId = uuid
                    )
                )
                return@put
            }

            if (!environment.flags.topicCreationEnabled) {
                log.info("Topic creation is disabled, checking if request wants to create new topic$logFormat", *logKeys)
                request.topics.map { it.topicName }
                    .filterNot { existingTopics.contains(it) }
                    .any {
                        val err = OneshotResponse(
                            status = OneshotStatus.ERROR,
                            message = "topic creation has been disabled, can't create topic $it",
                            requestId = uuid
                        )
                        call.respond(HttpStatusCode.Forbidden, err)
                        return@put
                    }
            }

            log.info("Checking if user has access to all topics in request$logFormat", *logKeys)
            request.topics.map { it.topicName }
                .filter { existingTopics.contains(it) }
                .filter { !ldap.userIsManager(it, currentUser) }
                .any {
                    val err = OneshotResponse(
                        status = OneshotStatus.ERROR,
                        message = "The user $currentUser does not have access to modify topic $it",
                        requestId = uuid
                    )
                    call.respond(HttpStatusCode.Unauthorized, err)
                    return@put
                }

            log.info("Validating topic names$logFormat", *logKeys)
            request.topics.map { it.topicName }.filterNot { it.isValidTopicName() }.any {
                val err = OneshotResponse(
                    status = OneshotStatus.ERROR,
                    message = "Invalid topic name - $it. Must contain [a..z]||[A..Z]||[0..9]||'-' only " +
                        "&& + length ≤ ${LDAPGroup.maxTopicNameLength()}",
                    requestId = uuid
                )
                call.respond(HttpStatusCode.BadRequest, err)
                return@put
            }

            log.info("Validating user names$logFormat", *logKeys)
            request.topics
                .flatMap {
                    it.members
                }
                .filterNot { ldap.userExists(it.member) }
                .any {
                    log.info("Tried to add the user ${it.member} who doesn't exist in AD$logFormat", *logKeys)
                    val err = OneshotResponse(
                        status = OneshotStatus.ERROR,
                        message = "The user ${it.member} does not exist",
                        requestId = uuid
                    )
                    call.respond(HttpStatusCode.BadRequest, err)
                    return@put
                }

            log.info("Validating member roles$logFormat", *logKeys)
            request.topics
                .flatMap { it.members }
                .filter { it.role == null } // Gson breaks non-nullability when values that don't exist in enums
                .any {
                    val validRoles: String = KafkaGroupType.values().joinToString(prefix = "[", postfix = "]")
                    val message: String = "Tried to add the user '${it.member}' with an invalid role (valid roles: $validRoles))"
                    log.info("$message $logFormat", *logKeys)
                    val err = OneshotResponse(
                        status = OneshotStatus.ERROR,
                        message = message,
                        requestId = uuid
                    )
                    call.respond(HttpStatusCode.BadRequest, err)
                    return@put
                }

            val groups = ldap.getKafkaGroups()
            log.debug("Getting members of topic groups$logFormat", *logKeys)
            val membersInGroup = request.topics
                .flatMap { topic ->
                    KafkaGroupType.values().flatMap { kafkaGroupType ->
                        val groupName = toGroupName(kafkaGroupType.prefix, topic.topicName)
                        ldap.getGroupMembers(groupName).map { member -> GroupMember(groupName, member) }
                    }
                }

            log.debug("Getting group members from request$logFormat", *logKeys)
            val groupNames = request.topics.flatMap { topic ->
                KafkaGroupType.values().map {
                    toGroupName(it.prefix, topic.topicName)
                }
            }

            val requestedGroupMembers = request.topics.flatMap { topic ->
                topic.members
                    .map { GroupMember(toGroupName(it.role.prefix, topic.topicName), it.member) }
                    .filterNot { requestGroupMember ->
                        requestGroupMember.group.contains(KafkaGroupType.MANAGER.prefix) && currentUser.equals(
                            requestGroupMember.user,
                            ignoreCase = true
                        )
                    }
                    .toMutableSet().apply {
                        val groupName = toGroupName(KafkaGroupType.MANAGER.prefix, topic.topicName)
                        add(GroupMember(groupName, currentUser))
                    }
            }

            // Add those who are missing from the group
            log.debug("Creating diff for creating + adding to group$logFormat", *logKeys)
            val groupAddDiff = requestedGroupMembers
                .filter { groupMember ->
                    val (group, member) = groupMember
                    membersInGroup
                        .filter { it.group.equals(group, ignoreCase = true) }
                        .none { it.user.equals(member, ignoreCase = true) }
                }
                .groupBy({ it.group }, { it.user })

            // Since we want to create all groups even though they don't have members we'll iterate over
            // group names that should be created, rather then using the data from the diff
            groupNames.forEach { group ->
                val groupMembers = groupAddDiff[group]
                if (!groups.contains(group)) {
                    log.info("Creating $group $logFormat", *logKeys)
                    ldap.createGroup(group)
                }
                if (groupMembers != null) {
                    log.info("Adding $groupMembers to $group $logFormat", *logKeys)
                    ldap.addToGroup(group, groupMembers)
                }
            }

            // Remove users that are not in the request from the group
            log.debug("Creating diff for removing group members$logFormat", *logKeys)
            membersInGroup
                .filter { inGroup ->
                    requestedGroupMembers
                        .filter { it.group.equals(inGroup.group, ignoreCase = true) }
                        .none { inGroup.user.equals(it.user, ignoreCase = true) }
                }
                .groupBy({ it.group }) { it.user }
                .filterNot { it.value.isEmpty() }
                .forEach {
                    val (group, members) = it
                    log.info("Removing $members from $group $logFormat", *logKeys)
                    ldap.removeGroupMembers(group, members)
                }

            val incrementallyUpdatedConfigurationsForTopics: Map<ConfigResource, List<AlterConfigOp>> = request.topics
                .filter { existingTopics.contains(it.topicName) }
                .associate { topicCreation ->
                    ConfigResource(ConfigResource.Type.TOPIC, topicCreation.topicName) to (
                        topicCreation.configEntries
                            ?: emptyMap()
                        )
                }
                .mapValues { (_, configEntries) ->
                    configEntries.map { (key, value) ->
                        AlterConfigOp(ConfigEntry(key, value.lowercase()), AlterConfigOp.OpType.SET)
                    }
                }

            // Alter configurations for existing topics
            log.debug("Altering configurations for existing topics$logFormat", *logKeys)
            adminClient?.incrementalAlterConfigs(incrementallyUpdatedConfigurationsForTopics)

            // Alter partition count for topic(s)
            try {
                val topicsAndPartitions: List<TopicPartitions> = request.topics
                    .filter { topic -> topic.topicName in existingTopics }
                    .map { topic ->
                        val partitions = adminClient.getTopicPartitions(topic.topicName, environment)
                        TopicPartitions(
                            topicName = topic.topicName,
                            existingCount = partitions.size,
                            requestedCount = topic.numPartitions
                        )
                    }
                if (topicsAndPartitions.isNotEmpty() && topicsAndPartitions.any { topic -> topic.requestedCount < topic.existingCount }) {
                    val violations: List<TopicPartitions> = topicsAndPartitions
                        .filter { topic -> topic.requestedCount < topic.existingCount }
                    val message = "Number of partitions for topic(s) cannot be reduced - $violations"
                    log.info(message)
                    call.respond(
                        HttpStatusCode.BadRequest,
                        OneshotResponse(
                            status = OneshotStatus.ERROR,
                            message = message,
                            requestId = uuid
                        )
                    )
                    return@put
                } else {
                    val topicsToAlter: Map<String, NewPartitions> = topicsAndPartitions
                        .filter { topic -> topic.requestedCount > topic.existingCount }
                        .map { topic ->
                            log.info("Increasing partition count for topic '${topic.topicName}' from '${topic.existingCount}' to '${topic.requestedCount}'")
                            topic.topicName to NewPartitions.increaseTo(topic.requestedCount)
                        }
                        .toMap()
                    adminClient?.createPartitions(topicsToAlter)
                }
            } catch (e: Exception) {
                log.error("Exception caught while increasing partitions for existing topic(s) $logFormat", logKeys, e)
                call.respond(
                    HttpStatusCode.ServiceUnavailable,
                    OneshotResponse(
                        status = OneshotStatus.ERROR,
                        message = "Failed to increase topic partitions in kafka",
                        requestId = uuid
                    )
                )
                return@put
            }

            // Create topics that are missing
            log.debug("Creating topics$logFormat", *logKeys)
            try {
                adminClient?.createTopics(
                    request.topics
                        .filterNot { existingTopics.contains(it.topicName) }
                        .map {
                            NewTopic(it.topicName, it.numPartitions, getDefaultReplicationFactor(adminClient, environment))
                                .configs(it.configEntries?.mapValues { it.value.lowercase() })
                        }
                )?.all()?.get(environment.kafka.kafkaTimeout, TimeUnit.MILLISECONDS)
            } catch (e: Exception) {
                log.error("Exception caught while creating topic(s), request: {} $logFormat", logKeys, e)
                call.respond(
                    HttpStatusCode.ServiceUnavailable,
                    OneshotResponse(
                        status = OneshotStatus.ERROR,
                        message = "Failed to create topic",
                        requestId = uuid
                    )
                )
            }

            log.debug("Creating ACLs$logFormat", *logKeys)
            val acl = request.topics
                .map { it.topicName }
                .flatMap { listOf(it to KafkaGroupType.CONSUMER, it to KafkaGroupType.PRODUCER) }
                .flatMap { (topic, groupType) -> groupType.intoAcls(topic) }

            try {
                adminClient?.createAcls(acl)?.all()?.get(environment.kafka.kafkaTimeout, TimeUnit.MILLISECONDS)
                log.info("Successfully updated acl for topic(s) - {} $logFormat", acl, logKeys)
            } catch (e: Exception) {
                log.error(
                    "Exception caught while creating ACL for topic(s), request: {} $logFormat", acl,
                    logKeys, e
                )
                call.respond(
                    HttpStatusCode.ServiceUnavailable,
                    OneshotResponse(
                        status = OneshotStatus.ERROR,
                        message = "Failed to create ACL for topic(s)",
                        requestId = uuid
                    )
                )
            }

            log.info("Fetching updated configurations$logFormat", *logKeys)
            val result = try {
                OneshotResult(uuid, request.topics.getCreationResult(adminClient, environment))
            } catch (e: Exception) {
                log.error(
                    "Exception caught while fetching updated configurations for topic(s), request: {} $logFormat",
                    logKeys, e
                )
                null
            }
            call.respond(
                OneshotResponse(
                    status = OneshotStatus.OK,
                    message = "Successfully created and/or updated topic(s)",
                    data = result,
                    requestId = uuid
                )
            )
        }
    }
}

private fun List<TopicCreation>.getCreationResult(
    adminClient: AdminClient?,
    environment: Environment
): List<TopicCreation> {
    return this.map { topicCreation ->
        val members: List<RoleMember> = LDAPGroup(environment)
            .use { ldap -> ldap.getKafkaGroupsAndMembers(topicCreation.topicName) }
            .flatMap { group ->
                group.members
                    .map { ldapMember -> // hack for fetching user OID/CN from ldap query string
                        ldapMember
                            .substringAfter("=")
                            .substringBefore(",")
                    }
                    .map { member -> Pair(member, group.type) }
            }
            .map { (member, type) -> RoleMember(member, type) }

        val partitions: Int = adminClient.getTopicPartitions(topicCreation.topicName, environment).size

        val configEntries: Map<String, String> = adminClient
            .getConfigEntries(topicCreation.topicName, environment)
            .map { entry -> Pair(entry.name(), entry.value().lowercase()) }
            .toMap()
            .flatMap { actualEntries ->
                topicCreation.configEntries
                    ?.filterKeys { expectedKey -> actualEntries.key == expectedKey }
                    ?.map { _ -> actualEntries.key to actualEntries.value }
                    ?: emptyList()
            }
            .toMap()

        TopicCreation(
            topicName = topicCreation.topicName,
            numPartitions = partitions,
            configEntries = configEntries,
            members = members
        )
    }
}

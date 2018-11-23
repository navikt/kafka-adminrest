package no.nav.integrasjon.api.v1

import io.ktor.application.call
import io.ktor.auth.UserIdPrincipal
import io.ktor.auth.principal
import io.ktor.http.HttpStatusCode
import io.ktor.locations.Location
import io.ktor.routing.Routing
import net.logstash.logback.argument.StructuredArguments.keyValue
import no.nav.integrasjon.FasitProperties
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.BasicAuthSecurity
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.Group
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.failed
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.ok
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.put
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.securityAndReponds
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.unAuthorized
import no.nav.integrasjon.ldap.KafkaGroupType
import no.nav.integrasjon.ldap.LDAPGroup
import no.nav.integrasjon.ldap.intoAcls
import no.nav.integrasjon.ldap.toGroupName
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.Config
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.config.ConfigResource
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.UUID

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

enum class OneshotStatus {
    ERROR,
    OK
}

data class GroupMember(val group: String, val user: String)

data class OneshotResponse(val status: OneshotStatus, val message: String, val data: OneshotResult? = null)
data class OneshotResult(val creationId: String)

@Group("Oneshot")
@Location(ONESHOT)
class Oneshot
fun Routing.registerOneshotApi(adminClient: AdminClient?, fasit: FasitProperties) {
    put<Oneshot, OneshotCreationRequest>(
            "Provides a one-shot kafka topic creation and ACL creation, it returns an ID and an endpoint where you can await a result from this request"
                    .securityAndReponds(BasicAuthSecurity(),
                            ok<TopicCreation>(),
                            failed<AnError>(),
                            unAuthorized<Unit>())) { _, request ->
        respondSelectiveCatch {
            val currentUser = call.principal<UserIdPrincipal>()!!.name

            val uuid = UUID.randomUUID().toString()

            val logKeys = arrayOf(
                    keyValue("requestId", uuid),
                    keyValue("currentUser", currentUser)
            )
            val logFormat = logKeys.joinToString(separator = ", ", prefix = "(", postfix = ")") { "{}" }

            log.info("Oneshot topic creation request initiated, request: $request $logFormat", *logKeys)

            LDAPGroup(fasit).use { ldap ->
                if (!ldap.userExists(currentUser))
                    return@respondSelectiveCatch HttpStatusCode.Unauthorized to OneshotResponse(
                            status = OneshotStatus.ERROR,
                            message = "authenticated user $currentUser doesn't exist as NAV ident or service user in " +
                                    "current LDAP domain, cannot be manager of topic"
                    )

                log.info("Validating config entries$logFormat", *logKeys)
                request.topics.flatMap { it.configEntries?.entries ?: setOf() }.filter { entry ->
                    AllowedConfigEntries.values().none { it.entryName == entry.key }
                }.any { return@respondSelectiveCatch HttpStatusCode.BadRequest to Exception("configEntry ${it.key} is not allowed to update automatically") }
                val existingTopics = adminClient?.listTopics()?.listings()?.get()?.map { it.name() } ?: emptyList()

                log.info("Checking if user has access to all topics in request$logFormat", *logKeys)
                request.topics.map { it.topicName }
                        .filter { existingTopics.contains(it) }
                        .filter { !ldap.userIsManager(it, currentUser) }
                        .any {
                            return@respondSelectiveCatch HttpStatusCode.Unauthorized to OneshotResponse(
                                    status = OneshotStatus.ERROR,
                                    message = "The user $currentUser does not have access to modify topic $it"
                            )
                        }

                log.info("Validating topic names$logFormat", *logKeys)
                request.topics.map { it.topicName }.filterNot { it.isValidTopicName() }.any {
                    return@respondSelectiveCatch HttpStatusCode.BadRequest to OneshotResponse(
                            status = OneshotStatus.ERROR,
                            message = "Invalid topic name - $it. Must contain [a..z]||[A..Z]||[0..9]||'-' only " +
                                    "&& + length ≤ ${LDAPGroup.maxTopicNameLength()}"
                    )
                }

                log.debug("Validating user names$logFormat", *logKeys)
                request.topics
                        .flatMap {
                            it.members
                        }
                        .filterNot { ldap.userExists(it.member) }
                        .any {
                            log.info("Tried to add the user ${it.member} who doesn't exist in AD$logFormat", *logKeys)
                            return@respondSelectiveCatch HttpStatusCode.BadRequest to OneshotResponse(
                                    status = OneshotStatus.ERROR,
                                    message = "The user ${it.member} does not exist"
                            )
                        }

                val groups = ldap.getKafkaGroups()
                log.debug("Getting members of topic groups$logFormat", *logKeys)
                val membersInGroup = request.topics
                        .flatMap { topic ->
                            KafkaGroupType.values().flatMap {
                                val groupName = toGroupName(it.prefix, topic.topicName)
                                ldap.getGroupMembers(groupName).map { GroupMember(groupName, it) }
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
                            .toMutableList().apply {
                                val groupName = toGroupName(KafkaGroupType.MANAGER.prefix, topic.topicName)
                                add(GroupMember(groupName, currentUser))
                            }
                }

                // Add those who are missing from the group
                log.debug("Creating diff for creating + adding to group$logFormat", *logKeys)
                val groupAddDiff = requestedGroupMembers
                        .filter {
                            val (group, member) = it
                            membersInGroup.filter { it.group.equals(group, ignoreCase = true) }
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

                // Create topics that are missing
                log.debug("Creating topics$logFormat", *logKeys)
                adminClient?.alterConfigs(request.topics.filter { existingTopics.contains(it.topicName) }
                        .map {
                            ConfigResource(ConfigResource.Type.TOPIC, it.topicName) to Config(it.configEntries
                                    ?.map { ConfigEntry(it.key, it.value) } ?: listOf())
                        }.toMap())

                adminClient?.createTopics(request.topics
                        .filterNot { existingTopics.contains(it.topicName) }
                        .map {
                            NewTopic(it.topicName, it.numPartitions, getDefaultReplicationFactor(adminClient))
                                    .configs(it.configEntries)
                        })?.all()?.get()

                log.debug("Creating ACLs$logFormat", *logKeys)
                val acl = request.topics
                        .map { it.topicName }
                        .flatMap { listOf(it to KafkaGroupType.CONSUMER, it to KafkaGroupType.PRODUCER) }
                        .flatMap { (topic, groupType) -> groupType.intoAcls(topic) }

                try {
                    adminClient?.createAcls(acl)?.all()?.get()
                    log.info("Successfully updated acl for topic(s) - {} $logFormat", acl, logKeys)
                    HttpStatusCode.OK to
                            OneshotResponse(OneshotStatus.OK, "Successfully created topic", OneshotResult(uuid))
                } catch (e: Exception) {
                    log.error("Exception caught while creating ACL for topic(s), request: {} $logFormat", acl,
                            logKeys, e)
                    HttpStatusCode.InternalServerError to
                            OneshotResponse(OneshotStatus.ERROR, "Failed to create topic")
                }
            }
        }
    }
}

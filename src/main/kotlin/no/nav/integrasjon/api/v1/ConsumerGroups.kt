package no.nav.integrasjon.api.v1

import io.ktor.application.ApplicationCall
import io.ktor.application.application
import io.ktor.application.call
import io.ktor.http.HttpStatusCode
import io.ktor.locations.Location
import io.ktor.response.respond
import io.ktor.routing.Routing
import io.ktor.util.pipeline.PipelineContext
import java.util.concurrent.TimeUnit
import no.nav.integrasjon.EXCEPTION
import no.nav.integrasjon.Environment
import no.nav.integrasjon.api.nais.client.SERVICES_ERR_K
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.Group
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.get
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.ok
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.responds
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.serviceUnavailable
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.ConsumerGroupDescription
import org.apache.kafka.clients.admin.MemberAssignment
import org.apache.kafka.clients.admin.MemberDescription
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.ConsumerGroupState
import org.apache.kafka.common.TopicPartition

fun Routing.consumerGroupsAPI(adminClient: AdminClient?, environment: Environment) {
    getConsumerGroup(adminClient, environment)
    getConsumerGroupMembers(adminClient, environment)
    getConsumerGroupOffsets(adminClient, environment)
    // todo putConsumerGroupOffsetForTopic(adminClient, environment)
}

private const val swGroup = "Consumer Groups (Group IDs)"

@Group(swGroup)
@Location("$CONSUMERGROUPS/{consumerGroup}")
data class GetConsumerGroup(val consumerGroup: String)

data class GetConsumerGroupModel(val name: String, val group: DescriptionWithDefaults)

data class DescriptionWithDefaults(
    val groupId: String = "",
    val isSimpleConsumerGroup: Boolean = true,
    val partitionAssignor: String = "",
    val state: ConsumerGroupState = ConsumerGroupState.UNKNOWN
)

fun Routing.getConsumerGroup(adminClient: AdminClient?, environment: Environment) =
    get<GetConsumerGroup>(
        "description for a consumer group".responds(
            ok<GetConsumerGroupModel>(),
            serviceUnavailable<AnError>()
        )
    ) { param ->
        val consumerGroupName = param.consumerGroup

        val (consumerGroupDescriptionRequestOk, consumerGroupDescription) = fetchConsumerGroupDescription(
            adminClient,
            environment,
            consumerGroupName
        )
        if (!consumerGroupDescriptionRequestOk) {
            call.respond(HttpStatusCode.ServiceUnavailable, AnError(SERVICES_ERR_K))
            return@get
        }

        call.respond(GetConsumerGroupModel(consumerGroupName, consumerGroupDescription))
    }

@Group(swGroup)
@Location("$CONSUMERGROUPS/{consumerGroup}/members")
data class GetConsumerGroupMembers(val consumerGroup: String)

data class GetConsumerGroupMembersModel(val name: String, val members: List<ConsumerGroupMemberDescription>)

data class ConsumerGroupMemberDescription(
    val memberId: String?,
    val groupInstanceId: String?,
    val clientId: String?,
    val host: String?,
    val assignment: MemberAssignment?
)

fun Routing.getConsumerGroupMembers(adminClient: AdminClient?, environment: Environment) =
    get<GetConsumerGroupMembers>(
        "members of a consumer group".responds(
            ok<GetConsumerGroupMembersModel>(),
            serviceUnavailable<AnError>()
        )
    ) { param ->
        val consumerGroupName = param.consumerGroup

        val (consumerGroupMembersRequestOk, consumerGroupMembers) = fetchConsumerGroupMembers(
            adminClient,
            environment,
            consumerGroupName
        )
        if (!consumerGroupMembersRequestOk) {
            call.respond(HttpStatusCode.ServiceUnavailable, AnError(SERVICES_ERR_K))
            return@get
        }

        call.respond(GetConsumerGroupMembersModel(consumerGroupName, consumerGroupMembers))
    }

@Group(swGroup)
@Location("$CONSUMERGROUPS/{consumerGroup}/offsets")
data class GetConsumerGroupOffsets(val consumerGroup: String)

data class GetConsumerGroupOffsetsModel(val name: String, val offsets: Map<TopicPartition, OffsetAndMetadata>)

fun Routing.getConsumerGroupOffsets(adminClient: AdminClient?, environment: Environment) =
    get<GetConsumerGroupOffsets>(
        "offsets for a consumer group".responds(
            ok<GetConsumerGroupOffsetsModel>(),
            serviceUnavailable<AnError>()
        )
    ) { param ->
        val consumerGroupName = param.consumerGroup

        val (consumerGroupRequestOk, consumerGroups) = fetchConsumerGroupOffsets(
            adminClient,
            environment,
            consumerGroupName
        )
        if (!consumerGroupRequestOk) {
            call.respond(HttpStatusCode.ServiceUnavailable, AnError(SERVICES_ERR_K))
            return@get
        }

        call.respond(GetConsumerGroupOffsetsModel(consumerGroupName, consumerGroups))
    }

private fun PipelineContext<Unit, ApplicationCall>.fetchConsumerGroupDescription(
    adminClient: AdminClient?,
    environment: Environment,
    consumerGroupName: String
): Pair<Boolean, DescriptionWithDefaults> {
    return try {
        Pair(true, adminClient?.let { ac ->
            ac.describeConsumerGroups(listOf(consumerGroupName))
                .all()
                .get(environment.kafka.kafkaTimeout, TimeUnit.MILLISECONDS)[consumerGroupName]
                .toSafeDeserializable()
        } ?: throw Exception(SERVICES_ERR_K)
        )
    } catch (e: Exception) {
        application.environment.log.error("$EXCEPTION get consumer group description request $consumerGroupName - $e")
        Pair(false, DescriptionWithDefaults())
    }
}

private fun PipelineContext<Unit, ApplicationCall>.fetchConsumerGroupMembers(
    adminClient: AdminClient?,
    environment: Environment,
    consumerGroupName: String
): Pair<Boolean, List<ConsumerGroupMemberDescription>> {
    return try {
        Pair(true, adminClient?.let { ac ->
            ac.describeConsumerGroups(listOf(consumerGroupName))
                .all()
                .get(environment.kafka.kafkaTimeout, TimeUnit.MILLISECONDS)[consumerGroupName]
                ?.members()
                ?.toList()?.map { it.toSafeDeserializable() }
                ?: emptyList()
        } ?: throw Exception(SERVICES_ERR_K)
        )
    } catch (e: Exception) {
        application.environment.log.error("$EXCEPTION get consumer group description request $consumerGroupName - $e")
        Pair(false, emptyList())
    }
}

private fun PipelineContext<Unit, ApplicationCall>.fetchConsumerGroupOffsets(
    adminClient: AdminClient?,
    environment: Environment,
    consumerGroupName: String
): Pair<Boolean, Map<TopicPartition, OffsetAndMetadata>> {
    return try {
        Pair(
            true, adminClient
                ?.listConsumerGroupOffsets(consumerGroupName)?.partitionsToOffsetAndMetadata()
                ?.get(environment.kafka.kafkaTimeout, TimeUnit.MILLISECONDS)
                ?: throw Exception(SERVICES_ERR_K)
        )
    } catch (e: Exception) {
        application.environment.log.error("$EXCEPTION get consumer group offsets request $consumerGroupName - $e")
        Pair(false, emptyMap())
    }
}

fun ConsumerGroupDescription?.toSafeDeserializable(): DescriptionWithDefaults =
    DescriptionWithDefaults(
        groupId = this?.groupId() ?: "",
        isSimpleConsumerGroup = this?.isSimpleConsumerGroup ?: true,
        partitionAssignor = this?.partitionAssignor() ?: "",
        state = this?.state() ?: ConsumerGroupState.UNKNOWN
    )

private fun MemberDescription.toSafeDeserializable(): ConsumerGroupMemberDescription =
    ConsumerGroupMemberDescription(
        memberId = this.consumerId(),
        groupInstanceId = this.groupInstanceId().orElse(null),
        clientId = this.clientId(),
        host = this.host(),
        assignment = this.assignment()
    )

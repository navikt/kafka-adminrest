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
import org.apache.kafka.common.ConsumerGroupState

fun Routing.consumerGroupsAPI(adminClient: AdminClient?, environment: Environment) {
    getConsumerGroup(adminClient, environment)
    getConsumerGroupOffsets(adminClient, environment)
}

private const val swGroup = "Consumer Groups"

@Group(swGroup)
@Location("$CONSUMERGROUPS/{groupId}")
data class GetConsumerGroup(val groupId: String)

data class GetConsumerGroupModel(
    val groupId: String = "",
    val isSimpleConsumerGroup: Boolean = true,
    val members: List<ConsumerGroupMemberDescription> = emptyList(),
    val partitionAssignor: String = "",
    val state: ConsumerGroupState = ConsumerGroupState.UNKNOWN
)

data class ConsumerGroupMemberDescription(
    val memberId: String?,
    val groupInstanceId: String?,
    val clientId: String?,
    val host: String?,
    val assignment: MemberAssignment?
)

fun Routing.getConsumerGroup(adminClient: AdminClient?, environment: Environment) =
    get<GetConsumerGroup>(
        "description and list of members for a consumer group".responds(
            ok<GetConsumerGroupModel>(),
            serviceUnavailable<AnError>()
        )
    ) { param ->
        val consumerGroupName = param.groupId

        val (consumerGroupDescriptionRequestOk, consumerGroupDescription) = fetchConsumerGroupDescription(
            adminClient,
            environment,
            consumerGroupName
        )
        if (!consumerGroupDescriptionRequestOk) {
            call.respond(HttpStatusCode.ServiceUnavailable, AnError(SERVICES_ERR_K))
            return@get
        }

        call.respond(consumerGroupDescription)
    }

@Group(swGroup)
@Location("$CONSUMERGROUPS/{groupId}/offsets")
data class GetConsumerGroupOffsets(val groupId: String)

data class GetConsumerGroupOffsetsModel(val name: String, val offsets: List<TopicPartitionOffsetAndMetadata>)

fun Routing.getConsumerGroupOffsets(adminClient: AdminClient?, environment: Environment) =
    get<GetConsumerGroupOffsets>(
        "offsets for a consumer group (for all topics and partitions the group is subscribed to)".responds(
            ok<GetConsumerGroupOffsetsModel>(),
            serviceUnavailable<AnError>()
        )
    ) { param ->
        val consumerGroupName = param.groupId

        val (consumerGroupOffsetRequestOk, consumerGroupOffsets) = fetchConsumerGroupOffsets(
            adminClient,
            environment,
            consumerGroupName
        )
        if (!consumerGroupOffsetRequestOk) {
            call.respond(HttpStatusCode.ServiceUnavailable, AnError(SERVICES_ERR_K))
            return@get
        }

        call.respond(GetConsumerGroupOffsetsModel(consumerGroupName, consumerGroupOffsets))
    }

private fun PipelineContext<Unit, ApplicationCall>.fetchConsumerGroupDescription(
    adminClient: AdminClient?,
    environment: Environment,
    consumerGroupName: String
): Pair<Boolean, GetConsumerGroupModel> {
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
        Pair(false, GetConsumerGroupModel())
    }
}

fun PipelineContext<Unit, ApplicationCall>.fetchConsumerGroupOffsets(
    adminClient: AdminClient?,
    environment: Environment,
    consumerGroupName: String
): Pair<Boolean, List<TopicPartitionOffsetAndMetadata>> {
    return try {
        Pair(
            true, adminClient
                ?.listConsumerGroupOffsets(consumerGroupName)?.partitionsToOffsetAndMetadata()
                ?.get(environment.kafka.kafkaTimeout, TimeUnit.MILLISECONDS)
                ?.toTopicPartitionOffsetAndMetadata()
                ?: throw Exception(SERVICES_ERR_K)
        )
    } catch (e: Exception) {
        application.environment.log.error("$EXCEPTION get consumer group offsets request $consumerGroupName - $e")
        Pair(false, emptyList())
    }
}

fun ConsumerGroupDescription?.toSafeDeserializable(): GetConsumerGroupModel =
    GetConsumerGroupModel(
        groupId = this?.groupId() ?: "",
        isSimpleConsumerGroup = this?.isSimpleConsumerGroup ?: true,
        members = this?.members()?.toList()?.map { it.toSafeDeserializable() } ?: emptyList(),
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

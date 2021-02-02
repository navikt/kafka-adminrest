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
import org.apache.kafka.common.Node
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.acl.AclOperation

fun Routing.consumerGroupsAPI(adminClient: AdminClient?, environment: Environment) {
    getConsumerGroupOffsets(adminClient, environment)
}

private const val swGroup = "Consumer Groups"

@Group(swGroup)
@Location("$CONSUMERGROUPS/{consumerGroup}/offsets")
data class GetConsumerGroupOffsets(val consumerGroup: String)

data class GetConsumerGroupOffsetsModel(val name: String, val group: ConsumerGroupsWithOffsets)

data class ConsumerGroupsWithOffsets(
    val description: DescriptionWithDefaults,
    val offsets: Map<TopicPartition, OffsetAndMetadata>
) {
    data class DescriptionWithDefaults(
        val groupId: String = "",
        val isSimpleConsumerGroup: Boolean = true,
        val members: List<MemberDescription> = emptyList(),
        val partitionAssignor: String = "",
        val state: ConsumerGroupState = ConsumerGroupState.UNKNOWN,
        val coordinator: Node? = null,
        val authorizedOperations: Set<AclOperation> = emptySet()
    ) {
        data class MemberDescription(
            val memberId: String?,
            val groupInstanceId: String?,
            val clientId: String?,
            val host: String?,
            val assignment: MemberAssignment?
        )
    }
}

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

private fun PipelineContext<Unit, ApplicationCall>.fetchConsumerGroupOffsets(
    adminClient: AdminClient?,
    environment: Environment,
    consumerGroupName: String
): Pair<Boolean, ConsumerGroupsWithOffsets> {
    return try {
        Pair(true, adminClient?.let { ac ->
            val offsets: Map<TopicPartition, OffsetAndMetadata> = ac.listConsumerGroupOffsets(consumerGroupName)
                .partitionsToOffsetAndMetadata()
                .get(environment.kafka.kafkaTimeout, TimeUnit.MILLISECONDS)

            val description: Map<String, ConsumerGroupDescription> =
                ac.describeConsumerGroups(listOf(consumerGroupName))
                    .all()
                    .get(environment.kafka.kafkaTimeout, TimeUnit.MILLISECONDS)

            ConsumerGroupsWithOffsets(description[consumerGroupName].toSafeDeserializable(), offsets)
        } ?: throw Exception(SERVICES_ERR_K)
        )
    } catch (e: Exception) {
        application.environment.log.error("$EXCEPTION get consumer group offsets request $consumerGroupName - $e")
        Pair(false, ConsumerGroupsWithOffsets(ConsumerGroupsWithOffsets.DescriptionWithDefaults(), emptyMap()))
    }
}

private fun ConsumerGroupDescription?.toSafeDeserializable(): ConsumerGroupsWithOffsets.DescriptionWithDefaults =
    ConsumerGroupsWithOffsets.DescriptionWithDefaults(
        groupId = this?.groupId() ?: "",
        isSimpleConsumerGroup = this?.isSimpleConsumerGroup ?: true,
        members = this?.members()?.toList()?.map { it.toSafeDeserializable() } ?: emptyList(),
        partitionAssignor = this?.partitionAssignor() ?: "",
        state = this?.state() ?: ConsumerGroupState.UNKNOWN,
        coordinator = this?.coordinator(),
        authorizedOperations = this?.authorizedOperations() ?: emptySet()
    )

private fun MemberDescription.toSafeDeserializable(): ConsumerGroupsWithOffsets.DescriptionWithDefaults.MemberDescription =
    ConsumerGroupsWithOffsets.DescriptionWithDefaults.MemberDescription(
        memberId = this.consumerId(),
        groupInstanceId = this.groupInstanceId().orElse(null),
        clientId = this.clientId(),
        host = this.host(),
        assignment = this.assignment()
    )

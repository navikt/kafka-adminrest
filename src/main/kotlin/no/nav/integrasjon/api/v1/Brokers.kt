package no.nav.integrasjon.api.v1

import io.ktor.locations.Location
import io.ktor.routing.Routing
import no.nav.integrasjon.Environment
import no.nav.integrasjon.api.nais.client.SERVICES_ERR_K
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.Group
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.get
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.ok
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.responds
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.serviceUnavailable
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.common.Node
import org.apache.kafka.common.config.ConfigResource
import java.util.concurrent.TimeUnit

/**
 * Brokers API
 * just a couple of read only routes
 * - get all brokers in cluster
 * - get config. details for a specific broker
 */

// a wrapper for this api to be installed as routes
fun Routing.brokersAPI(adminClient: AdminClient?, environment: Environment) {

    getBrokers(adminClient, environment)
    getBrokerConfig(adminClient, environment)
}

private const val swGroup = "Brokers"

/**
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/admin/AdminClient.html#describeCluster--
 */

@Group(swGroup)
@Location(BROKERS)
class GetBrokers

data class GetBrokersModel(val brokers: List<Node>)

fun Routing.getBrokers(adminClient: AdminClient?, environment: Environment) =
    get<GetBrokers>("all brokers".responds(ok<GetBrokersModel>(), serviceUnavailable<AnError>())) {
        respondOrServiceUnavailable {

            val nodes = adminClient
                ?.describeCluster()
                ?.nodes()
                ?.get(environment.kafka.kafkaTimeout, TimeUnit.MILLISECONDS)
                ?.toList()
                ?: throw Exception(SERVICES_ERR_K)

            GetBrokersModel(nodes)
        }
    }

/**
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/admin/AdminClient.html#describeConfigs-java.util.Collection-
 */

@Group(swGroup)
@Location("$BROKERS/{brokerID}")
data class GetBrokerConfig(val brokerID: String)

data class GetBrokerConfigModel(val id: String, val config: List<ConfigEntry>)

fun Routing.getBrokerConfig(adminClient: AdminClient?, environment: Environment) =
    get<GetBrokerConfig>(
        "a broker configuration".responds(ok<GetBrokerConfigModel>(),
            serviceUnavailable<AnError>())) { broker ->
        respondOrServiceUnavailable {

            val brokerConfig = adminClient
                ?.describeConfigs(listOf(ConfigResource(ConfigResource.Type.BROKER, broker.brokerID)))
                ?.all()
                ?.get(environment.kafka.kafkaTimeout, TimeUnit.MILLISECONDS)
                ?.entries
                ?.first()
                ?.value
                ?.entries()
                ?.toList()
                ?: throw Exception(SERVICES_ERR_K)

            GetBrokerConfigModel(broker.brokerID, brokerConfig)
        }
    }

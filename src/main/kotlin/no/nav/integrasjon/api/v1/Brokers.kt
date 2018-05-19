package no.nav.integrasjon.api.v1

import io.ktor.application.call
import io.ktor.routing.Routing
import io.ktor.routing.get
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.config.ConfigResource

fun Routing.brokersAPI(adminClient: AdminClient) {

    getBrokers(adminClient)
    getBrokerConfig(adminClient)
}

fun Routing.getBrokers(adminClient: AdminClient) =
        get(BROKERS) {
            respondAndCatch {
                adminClient.describeCluster().nodes().get()
            }
        }

fun Routing.getBrokerConfig(adminClient: AdminClient) =
        get("$BROKERS/{brokerID}") {
            respondAndCatch {
                adminClient.describeConfigs(
                        mutableListOf(
                                ConfigResource(
                                        ConfigResource.Type.BROKER,
                                        call.parameters["brokerID"]
                                )
                        )
                ).values().entries.map { Pair(it.key,it.value.get()) }
            }
        }
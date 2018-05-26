package no.nav.integrasjon.api.v1

import io.ktor.application.call
import io.ktor.routing.Routing
import io.ktor.routing.get
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.config.ConfigResource

/**
 * Brokers API
 * just a couple of read only routes
 * - get all brokers in cluster
 * - get config. details for a specific broker
 */

// a wrapper for this api to be installed as routes
fun Routing.brokersAPI(adminClient: AdminClient) {

    getBrokers(adminClient)
    getBrokerConfig(adminClient)
}

/**
 * GET https://<host>/api/v1/brokers
 *
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/admin/AdminClient.html#describeCluster--
 *
 * Returns a collection of org.apache.kafka.common.Node
 *
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/common/Node.html
 */
fun Routing.getBrokers(adminClient: AdminClient) =
        get(BROKERS) {
            respondCatch {
                adminClient.describeCluster()
                        .nodes()
                        .get()
            }
        }

/**
 * GET https://<host>/api/v1/brokers/{brokerID}
 *
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/admin/AdminClient.html#describeConfigs-java.util.Collection-
 *
 * Returns a map of org.apache.kafka.clients.admin.Config for the given resource
 *
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/admin/Config.html
 */
fun Routing.getBrokerConfig(adminClient: AdminClient) =
        get("$BROKERS/{brokerID}") {
            respondCatch {
                adminClient.describeConfigs(
                        listOf(
                                ConfigResource(
                                        ConfigResource.Type.BROKER,
                                        call.parameters["brokerID"]
                                )
                        )
                )
                        .all()
                        .get()
            }
        }
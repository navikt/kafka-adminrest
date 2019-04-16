package no.nav.integrasjon.api.nais.client

import io.ktor.application.call
import io.ktor.http.ContentType
import io.ktor.response.respondText
import io.ktor.response.respondTextWriter
import io.ktor.routing.Routing
import io.ktor.routing.get
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.exporter.common.TextFormat
import no.nav.integrasjon.Environment
import no.nav.integrasjon.api.v1.NAIS_ISALIVE
import no.nav.integrasjon.api.v1.NAIS_ISREADY
import no.nav.integrasjon.api.v1.backEndServicesAreOk
import no.nav.integrasjon.api.v1.respondOrServiceUnavailable
import org.apache.kafka.clients.admin.AdminClient

/**
 * NAIS API
 *
 * supporting the mandatory NAIS services
 * - getIsAlive
 * - getIsReady
 * - getPrometheus
 */

const val SERVICES_ERR_GAK = "All services unavailable (ldap group and authentication, and kafka)"
internal const val SERVICES_ERR_GA = "ldap group and authentication are not available "
internal const val SERVICES_ERR_GK = "ldap group and kafka are not available"
const val SERVICES_ERR_G = "ldap group is not available"
internal const val SERVICES_ERR_AK = "ldap authentication and kafka are not available"
const val SERVICES_ERR_A = "ldap authentication is not available"
const val SERVICES_ERR_K = "kafka is not available"
internal const val SERVICES_ERR_STRANGE = "something strange - see function 'getIsReady in naisAPI'"
internal const val SERVICES_OK = "is ready"

// a wrapper for this api to be installed as routes
fun Routing.naisAPI(adminClient: AdminClient?, environment: Environment, collectorRegistry: CollectorRegistry) {

    getIsAlive()
    getIsReady(adminClient, environment)
    getPrometheus(collectorRegistry)
}

// isAlive don't need any tests, fasit properties are ok if this route is active
fun Routing.getIsAlive() =
        get(NAIS_ISALIVE) {
            call.respondText("is alive", ContentType.Text.Plain)
        }

fun Routing.getIsReady(adminClient: AdminClient?, environment: Environment) =
        get(NAIS_ISREADY) {
            respondOrServiceUnavailable {

                val (ldapGroupIsOK, ldapAuthenIsOk, kafkaIsOk) = backEndServicesAreOk(adminClient, environment)

                val msg = when {
                    !(ldapGroupIsOK || ldapAuthenIsOk || kafkaIsOk) -> SERVICES_ERR_GAK
                    !(ldapGroupIsOK || ldapAuthenIsOk) && kafkaIsOk -> SERVICES_ERR_GA
                    !ldapGroupIsOK && ldapAuthenIsOk && !kafkaIsOk -> SERVICES_ERR_GK
                    !ldapGroupIsOK && ldapAuthenIsOk && kafkaIsOk -> SERVICES_ERR_G
                    ldapGroupIsOK && !(ldapAuthenIsOk || kafkaIsOk) -> SERVICES_ERR_AK
                    ldapGroupIsOK && !ldapAuthenIsOk && kafkaIsOk -> SERVICES_ERR_A
                    ldapGroupIsOK && ldapAuthenIsOk && !kafkaIsOk -> SERVICES_ERR_K
                    ldapGroupIsOK && ldapAuthenIsOk && kafkaIsOk -> SERVICES_OK
                    else -> SERVICES_ERR_STRANGE
                    }

                if (ldapGroupIsOK && ldapAuthenIsOk && kafkaIsOk) msg else throw Exception(msg)
            }
        }

fun Routing.getPrometheus(collectorRegistry: CollectorRegistry) =
        get("/prometheus") {
            val names = call.request.queryParameters.getAll("name[]")?.toSet() ?: setOf()
            call.respondTextWriter(ContentType.parse(TextFormat.CONTENT_TYPE_004)) {
                TextFormat.write004(
                        this,
                        collectorRegistry.filteredMetricFamilySamples(names))
            }
        }
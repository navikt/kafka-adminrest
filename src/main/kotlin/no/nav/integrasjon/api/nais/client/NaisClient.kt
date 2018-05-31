package no.nav.integrasjon.api.nais.client

import io.ktor.application.call
import io.ktor.http.ContentType
import io.ktor.response.respondText
import io.ktor.response.respondWrite
import io.ktor.routing.Routing
import io.ktor.routing.get
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.exporter.common.TextFormat
import no.nav.integrasjon.FasitProperties
import no.nav.integrasjon.api.v1.respondCatch
import no.nav.integrasjon.ldap.LDAPAuthenticate
import no.nav.integrasjon.ldap.LDAPGroup
import org.apache.kafka.clients.admin.AdminClient

/**
 * NAIS API
 *
 * supporting the mandatory NAIS services
 * - getIsAlive
 * - getIsReady
 * - getPrometheus
 */

// a wrapper for this api to be installed as routes
fun Routing.naisAPI(adminClient: AdminClient, config: FasitProperties, collectorRegistry: CollectorRegistry) {

    getIsAlive()
    getIsReady(adminClient, config)
    getPrometheus(collectorRegistry)
}

// isAlive don't need any tests, fasit properties are ok if this route is active
fun Routing.getIsAlive() =
        get("/isAlive") {
            call.respondText("is alive", ContentType.Text.Plain)
        }

fun Routing.getIsReady(adminClient: AdminClient, config: FasitProperties) =
        get("/isReady") {
            respondCatch {
                if (LDAPGroup(config).use { ldapGroup -> ldapGroup.connectionOk } &&
                        LDAPAuthenticate(config).use { ldapAuthenticate -> ldapAuthenticate.connectionOk } &&
                        adminClient.listTopics().namesToListings().get().isNotEmpty()
                )
                    "is ready"
                else
                    "is not ready"
            }
        }

fun Routing.getPrometheus(collectorRegistry: CollectorRegistry) =
        get("/prometheus") {
            val names = call.request.queryParameters.getAll("name[]")?.toSet() ?: setOf()
            call.respondWrite(ContentType.parse(TextFormat.CONTENT_TYPE_004)) {
                TextFormat.write004(
                        this,
                        collectorRegistry.filteredMetricFamilySamples(names))
            }
        }
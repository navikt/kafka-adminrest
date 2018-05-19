package no.nav.integrasjon.api.nais.client

import io.ktor.application.call
import io.ktor.http.ContentType
import io.ktor.response.respondText
import io.ktor.response.respondWrite
import io.ktor.routing.Routing
import io.ktor.routing.get
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.exporter.common.TextFormat

// TODO - isAlive must be related to successful start of KTOR server or not
fun Routing.getIsAlive() =
        get("/isAlive") {
            call.respondText("is alive", ContentType.Text.Plain)
        }

// TODO - isReady iff both adminclient and ldap APIs are successfully connected
fun Routing.getIsReady() =
        get("/isReady") {
            call.respondText("is ready", ContentType.Text.Plain)
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
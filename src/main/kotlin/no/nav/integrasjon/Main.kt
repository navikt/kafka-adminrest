package no.nav.integrasjon

import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import mu.KotlinLogging

fun main() {

    val log = KotlinLogging.logger { }

    log.info { "Checking Environment properties" }
    val environment = Environment()
    if (!environment.ldapAuthenticate.infoComplete()) {
        log.error { "Incomplete properties for ldap authentication - $environment" }
        return
    }

    if (!environment.ldapGroup.infoComplete()) {
        log.error { "Incomplete properties for ldap group management - $environment" }
        return
    }

    if (environment.kafka.securityEnabled() && !environment.kafka.securityComplete()) {
        log.error { "Kafka security enabled, but incomplete kafka security properties - $environment" }
        return
    }

    // see https://ktor.io/index.html for ktor enlightenment
    // start embedded netty, then fire opp ktor module and wait for connections
    embeddedServer(Netty, 8080, module = { kafkaAdminREST(environment) }).start(wait = true)
}
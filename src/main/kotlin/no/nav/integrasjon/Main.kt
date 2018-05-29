package no.nav.integrasjon

import io.ktor.application.Application
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import mu.KotlinLogging

fun main(args: Array<String>) {

    val log = KotlinLogging.logger {  }

    log.info { "Checking Fasit properties" }
    FasitProperties().let { fp ->
        if (!fp.ldapAuthenticationInfoComplete()) {
            log.error { "Incomplete properties for ldap authentication - $fp" }
            return
        }

        if (!fp.ldapGroupInfoComplete()) {
            log.error { "Incomplete properties for ldap group management - $fp" }
            return
        }

        if (fp.kafkaSecurityEnabled() && !fp.kafkaSecurityComplete()) {
            log.error { "Kafka security enabled, but incomplete kafka security properties - $fp" }
            return
        }
    }

    // see https://ktor.io/index.html for ktor enlightenment
    // start embedded netty, then fire opp ktor module and wait for connections
    embeddedServer(Netty, 8080, module = Application::kafkaAdminREST).start(wait = true)
}
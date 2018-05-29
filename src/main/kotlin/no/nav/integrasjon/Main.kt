package no.nav.integrasjon

import com.unboundid.ldap.sdk.LDAPConnection
import com.unboundid.util.ssl.SSLUtil
import com.unboundid.util.ssl.TrustAllTrustManager
import io.ktor.application.Application
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import mu.KotlinLogging
import no.nav.integrasjon.ldap.LDAPBase

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
    //embeddedServer(Netty, 8080, module = Application::kafkaAdminREST).start(wait = true)

    LDAPConnection(SSLUtil(TrustAllTrustManager()).createSSLSocketFactory()).apply {
        connect("localhost",10636)
    }.use {
        it.bind("uid=srvkafkabroker,ou=users,dc=security,dc=example,dc=com","broker")
        System.out.println(it.getSchema().getSchemaEntry().toLDIFString())
        println(it.getSchema().schemaEntry.toLDIF())
    }
}
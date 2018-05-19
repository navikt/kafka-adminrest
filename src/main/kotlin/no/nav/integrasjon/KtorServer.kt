package no.nav.integrasjon

import io.ktor.application.*
import io.ktor.auth.Authentication
import io.ktor.auth.UserIdPrincipal
import io.ktor.auth.basic
import io.ktor.features.*
import io.ktor.gson.gson
import io.ktor.http.*
import io.ktor.request.path
import io.ktor.response.*
import io.ktor.routing.Routing
import io.ktor.util.*
import io.prometheus.client.CollectorRegistry
import mu.KotlinLogging
import no.nav.integrasjon.api.nais.client.getIsAlive
import no.nav.integrasjon.api.nais.client.getIsReady
import no.nav.integrasjon.api.nais.client.getPrometheus
import no.nav.integrasjon.api.v1.API_V1
import no.nav.integrasjon.api.v1.aclAPI
import no.nav.integrasjon.api.v1.topicsAPI
import no.nav.integrasjon.api.v1.brokersAPI
import no.nav.integrasjon.api.v1.groupsAPI
import no.nav.integrasjon.ldap.LDAPBase
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.slf4j.event.Level
import java.util.*

const val AUTHENTICATION_BASIC = "basicAuth"

fun Application.main() {

    val log = KotlinLogging.logger {  }

    log.info { "Starting ktor server" }

    log.info { "Checking Fasit properties" }
    val adminClient = FasitProperties().let { fp ->

        if (fp.isEmpty()) {
            log.error { "Missing fasit properties - $fp" }
            return
        }

        if (fp.kafkaSecurityEnabled() && !fp.kafkaSecurityComplete()) {
            log.error { "Kafka security enabled, but incomplete kafka security settings - $fp" }
            return
        }

        /**
         * All kafka operations are managed via kafka AdminClient
         * See https://kafka.apache.org/10/javadoc/index.html?org/apache/kafka/clients/admin/AdminClient.html
         */
        log.info { "Creating kafka admin client" }

        AdminClient.create(Properties()
                .apply {
                    set(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, fp.kafkaBrokers)
                    set(ConsumerConfig.CLIENT_ID_CONFIG, fp.kafkaClientID)
                    if (fp.kafkaSecurityEnabled()) {
                        set("security.protocol", fp.kafkaSecProt)
                        set("sasl.mechanism", fp.kafkaSaslMec)
                        set("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                                "username=\"${fp.kafkaUser}\" password=\"${fp.kafkaPassword}\";")
                    }
                })
    }

    val collectorRegistry = CollectorRegistry.defaultRegistry

    log.info { "Installing ktor features" }
    install(DefaultHeaders)
    install(ConditionalHeaders)
    install(Compression)
    install(AutoHeadResponse)
    install(CallLogging) {
        level = Level.INFO
        filter { call -> call.request.path().startsWith(API_V1) }
    }
    //install(XForwardedHeadersSupport) - is this needed, and supported in reverse proxy in matter?
    install(StatusPages) {
        exception<Throwable> { cause ->
            environment.log.error(cause)
            call.respond(HttpStatusCode.InternalServerError)
        }
    }

    install(Authentication) {
        basic(name = AUTHENTICATION_BASIC) {
            realm = "Ktor Server"
            validate { credentials ->
                LDAPBase(FasitProperties()).use { ldap ->
                        if (ldap.canUserAuthenticate(credentials.name, credentials.password))
                            UserIdPrincipal(credentials.name)
                        else
                            null
                }
            }
        }
    }

    install(ContentNegotiation) {
        gson {
            serializeNulls()
        }
    }

    log.info { "Installing ktor routes" }
    install(Routing) {
        // support classic nais requirements
        getIsAlive()
        getIsReady()
        getPrometheus(collectorRegistry)

        // provide the essential, management of kafka environment, topic creation and authorization
        topicsAPI(adminClient, FasitProperties())
        brokersAPI(adminClient)
        aclAPI(adminClient)
        groupsAPI(FasitProperties())
    }
}



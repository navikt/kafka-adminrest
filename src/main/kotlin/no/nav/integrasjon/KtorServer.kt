package no.nav.integrasjon

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.*
import io.ktor.application.*
import io.ktor.features.*
import io.ktor.gson.gson
import io.ktor.http.*
import io.ktor.response.*
import io.ktor.routing.Routing
import io.ktor.util.*
import io.prometheus.client.CollectorRegistry
import mu.KotlinLogging
import no.nav.integrasjon.api.nais.client.getIsAlive
import no.nav.integrasjon.api.nais.client.getIsReady
import no.nav.integrasjon.api.nais.client.getPrometheus
import no.nav.integrasjon.api.v1.adminclient.kafkaAPI
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.slf4j.event.Level
import java.util.*


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
            log.error { "Kafka security enable, but incomplete kafka security settings - $fp" }
            return
        }

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
    }
    install(XForwardedHeadersSupport)
    install(StatusPages) {
        exception<Throwable> { cause ->
            environment.log.error(cause)
            call.respond(HttpStatusCode.InternalServerError)
        }
    }

    install(ContentNegotiation) {
        gson {
            serializeNulls()
        }
    }

    log.info { "Installing ktor routes" }
    install(Routing) {
        getIsAlive()
        getIsReady()
        getPrometheus(collectorRegistry)

        kafkaAPI(adminClient)
    }
}



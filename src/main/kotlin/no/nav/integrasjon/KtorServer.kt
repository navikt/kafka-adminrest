package no.nav.integrasjon

import io.ktor.application.Application
import io.ktor.application.call
import io.ktor.application.install
import io.ktor.auth.Authentication
import io.ktor.auth.UserIdPrincipal
import io.ktor.auth.basic
import io.ktor.features.AutoHeadResponse
import io.ktor.features.CallLogging
import io.ktor.features.Compression
import io.ktor.features.ConditionalHeaders
import io.ktor.features.ContentNegotiation
import io.ktor.features.DefaultHeaders
import io.ktor.features.StatusPages
import io.ktor.gson.gson
import io.ktor.http.HttpStatusCode
import io.ktor.locations.Locations
import io.ktor.request.path
import io.ktor.response.respond
import io.ktor.response.respondRedirect
import io.ktor.routing.Routing
import io.ktor.routing.get
import io.ktor.util.error
import io.prometheus.client.CollectorRegistry
import mu.KotlinLogging
import no.nav.integrasjon.api.nais.client.naisAPI
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.Contact
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.Information
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.Swagger
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.SwaggerUi
import no.nav.integrasjon.api.v1.API_V1
import no.nav.integrasjon.api.v1.topicsAPI
import no.nav.integrasjon.api.v1.brokersAPI
import no.nav.integrasjon.api.v1.groupsAPI
import no.nav.integrasjon.api.v1.aclAPI
import no.nav.integrasjon.ldap.LDAPAuthenticate
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.slf4j.event.Level
import java.util.Properties

const val AUTHENTICATION_BASIC = "basicAuth"

val swagger = Swagger(
        info = Information(
                version = "0.51-SNAPSHOT",
                title = "Kafka self service API",
                description = "[kafka-adminrest](https://github.com/navikt/kafka-adminrest)",
                contact = Contact(
                    name = "Torstein Nesby, Trong Huu Nguyen",
                    url = "https://github.com/navikt/kafka-adminrest",
                    email = "")
        )
)

/**
 * Application.kafkaAdminREST is bootstrapping the already startet Netty server
 * with suitable set of functionality
 */

fun Application.kafkaAdminREST() {

    val log = KotlinLogging.logger { }

    log.info { "Starting server" }

    val fasitProps = FasitPropFactory.fasitProperties
    val adminClient = fasitProps.let { fp ->

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

    log.info { "Installing features" }
    install(DefaultHeaders)
    install(ConditionalHeaders)
    install(Compression)
    install(AutoHeadResponse)
    install(CallLogging) {
        level = Level.INFO
        filter { call -> call.request.path().startsWith(API_V1) }
    }
    // install(XForwardedHeadersSupport) - is this needed, and supported in reverse proxy in matter?
    install(StatusPages) {
        exception<Throwable> { cause ->
            environment.log.error(cause)
            call.respond(HttpStatusCode.InternalServerError)
        }
    }
    install(Authentication) {
        basic(name = AUTHENTICATION_BASIC) {
            realm = "kafka-adminrest"
            validate { credentials ->
                LDAPAuthenticate(fasitProps).use { ldap ->
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
    install(Locations)

    val swaggerUI = SwaggerUi()

    log.info { "Installing routes" }
    install(Routing) {

        // swagger UI trigger routes
        get(API_V1) { call.respondRedirect("$API_V1/apidocs/index.html?url=swagger.json") }
        get("$API_V1/apidocs/{fileName}") {
            val fileName = call.parameters["fileName"]
            if (fileName == "swagger.json") call.respond(swagger) else swaggerUI.serve(fileName, call)
        }

        // support classic nais requirements
        naisAPI(adminClient, fasitProps, collectorRegistry)

        // provide the essential, management of kafka environment, topic creation and authorization
        topicsAPI(adminClient, fasitProps)
        brokersAPI(adminClient)
        aclAPI(adminClient)
        groupsAPI(fasitProps)
    }
}

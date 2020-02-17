package no.nav.integrasjon

import com.google.gson.JsonSyntaxException
import io.ktor.application.Application
import io.ktor.application.call
import io.ktor.application.install
import io.ktor.auth.Authentication
import io.ktor.auth.UserIdPrincipal
import io.ktor.auth.basic
import io.ktor.features.AutoHeadResponse
import io.ktor.features.CallId
import io.ktor.features.CallLogging
import io.ktor.features.Compression
import io.ktor.features.ConditionalHeaders
import io.ktor.features.ContentNegotiation
import io.ktor.features.DefaultHeaders
import io.ktor.features.StatusPages
import io.ktor.features.callIdMdc
import io.ktor.gson.gson
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.locations.KtorExperimentalLocationsAPI
import io.ktor.locations.Locations
import io.ktor.request.path
import io.ktor.response.respond
import io.ktor.response.respondRedirect
import io.ktor.routing.Routing
import io.ktor.routing.get
import io.ktor.util.error
import io.prometheus.client.CollectorRegistry
import java.util.Properties
import java.util.UUID
import mu.KotlinLogging
import no.nav.integrasjon.api.nais.client.naisAPI
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.Contact
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.Information
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.Swagger
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.SwaggerUi
import no.nav.integrasjon.api.v1.API_V1
import no.nav.integrasjon.api.v1.aclAPI
import no.nav.integrasjon.api.v1.apigwAPI
import no.nav.integrasjon.api.v1.brokersAPI
import no.nav.integrasjon.api.v1.groupsAPI
import no.nav.integrasjon.api.v1.registerOneshotApi
import no.nav.integrasjon.api.v1.streamsAPI
import no.nav.integrasjon.api.v1.topicsAPI
import no.nav.integrasjon.ldap.LDAPAuthenticate
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.slf4j.event.Level

const val AUTHENTICATION_BASIC = "basicAuth"

val swagger = Swagger(
    info = Information(
        version = System.getenv("APP_VERSION")?.toString() ?: "",
        title = "Kafka self service API",
        description = "[kafka-adminrest](https://github.com/navikt/kafka-adminrest)",
        contact = Contact(
            name = "Torstein Nesby, Trong Huu Nguyen, Kevin Sillerud",
            url = "https://github.com/navikt/kafka-adminrest",
            email = ""
        )
    )
)

/**
 * Application.kafkaAdminREST is bootstrapping the already startet Netty server
 * with suitable set of functionality
 */

internal const val JAAS_PLAIN_LOGIN = "org.apache.kafka.common.security.plain.PlainLoginModule"
internal const val JAAS_REQUIRED = "required"
internal const val SWAGGER_URL_V1 = "$API_V1/apidocs/index.html?url=swagger.json"

@KtorExperimentalLocationsAPI
fun Application.kafkaAdminREST(environment: Environment) {

    val log = KotlinLogging.logger { }

    log.info { "Starting server" }

    val adminClient: AdminClient? = try {
        environment.let { env ->

            log.info { "Creating kafka admin client" }

            AdminClient.create(Properties()
                .apply {
                    set(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, env.kafka.kafkaBrokers)
                    set(ConsumerConfig.CLIENT_ID_CONFIG, env.kafka.kafkaClientID)
                    if (env.kafka.securityEnabled()) {
                        set(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, env.kafka.kafkaSecProt)
                        set(SaslConfigs.SASL_MECHANISM, env.kafka.kafkaSaslMec)
                        set(SaslConfigs.SASL_JAAS_CONFIG, "$JAAS_PLAIN_LOGIN $JAAS_REQUIRED " +
                            "username=\"${env.kafka.kafkaUser}\" password=\"${env.kafka.kafkaPassword}\";")
                    }
                })
        }
    } catch (e: Exception) {
        log.error(e) { "Could not initialize AdminClient" }
        null
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
        callIdMdc("callId")
    }
    // install(XForwardedHeadersSupport) - is this needed, and supported in reverse proxy in matter?
    install(StatusPages) {
        exception<Throwable> { cause ->
            log.error(cause)
            call.respond(HttpStatusCode.InternalServerError)
        }
        exception<JsonSyntaxException> { cause ->
            log.error(cause) {
                "Received an invalid input that could not be parsed as JSON"
            }
            call.respond(HttpStatusCode.BadRequest) {
                "The request could not be parsed as JSON. Check your input."
            }
        }
    }
    install(CallId) {
        generate { UUID.randomUUID().toString() }
        verify { callId: String -> callId.isNotEmpty() }
        header(HttpHeaders.XCorrelationId)
    }
    install(Authentication) {
        basic(name = AUTHENTICATION_BASIC) {
            realm = "kafka-adminrest"
            validate { credentials ->
                LDAPAuthenticate(environment).use { ldap ->
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
        get("/") { call.respondRedirect(SWAGGER_URL_V1) }
        get("/api") { call.respondRedirect(SWAGGER_URL_V1) }
        get("/api/v1") { call.respondRedirect(SWAGGER_URL_V1) }
        get("/api/v1/apidocs") { call.respondRedirect(SWAGGER_URL_V1) }
        get("$API_V1/apidocs/{fileName}") {
            val fileName = call.parameters["fileName"]
            if (fileName == "swagger.json") call.respond(swagger) else swaggerUI.serve(fileName, call)
        }

        // support classic nais requirements
        naisAPI(adminClient, environment, collectorRegistry)

        // provide the essential, management of kafka environment, topic creation and authorization

        streamsAPI(adminClient, environment)
        registerOneshotApi(adminClient, environment)
        topicsAPI(adminClient, environment)
        brokersAPI(adminClient, environment)
        aclAPI(adminClient, environment)
        groupsAPI(environment)
        apigwAPI(environment)
    }
}

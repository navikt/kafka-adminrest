package no.nav.integrasjon.api.v1

import io.ktor.application.ApplicationCall
import io.ktor.application.application
import io.ktor.application.call
import io.ktor.http.HttpStatusCode
import io.ktor.response.respond
import io.ktor.util.pipeline.PipelineContext
import java.util.concurrent.TimeUnit
import mu.KotlinLogging
import no.nav.integrasjon.EXCEPTION
import no.nav.integrasjon.Environment
import no.nav.integrasjon.api.nais.client.SERVICES_ERR_K
import no.nav.integrasjon.ldap.LDAPAuthenticate
import no.nav.integrasjon.ldap.LDAPGroup
import org.apache.kafka.clients.admin.AdminClient

private val logger = KotlinLogging.logger { }

// nais api
const val NAIS_ISALIVE = "/isAlive"
const val NAIS_ISREADY = "/isReady"

// route starting point
internal const val API_V1 = "/api/v1"

// route for brokers and acls in kafka environment, and LDAP groups
const val BROKERS = "$API_V1/brokers"
const val ACLS = "$API_V1/acls"
const val GROUPS = "$API_V1/groups"

// route for topics in kafka environment, and zoom into related acls and groups per topic
const val TOPICS = "$API_V1/topics"
const val ONESHOT = "$API_V1/oneshot"

// Route for streams
const val STREAMS = "$API_V1/streams"

// Route for apigw
const val APIGW = "$API_V1/apigw"

// simple data class for exceptions
data class AnError(val error: String)

internal fun kafkaIsOk(adminClient: AdminClient?, environment: Environment): Boolean =
    try {
        adminClient
            ?.listTopics()
            ?.namesToListings()
            ?.get()?.isNotEmpty() ?: false
    } catch (e: Exception) {
        logger.error(e) { "Could not connect to kafka: timeout - ${environment.kafka.kafkaTimeout}" }
        false
    }

internal fun backEndServicesAreOk(
    adminClient: AdminClient?,
    environment: Environment
): Triple<Boolean, Boolean, Boolean> = Triple(
    LDAPGroup(environment).use { ldapGroup -> ldapGroup.connectionOk },
    LDAPAuthenticate(environment).use { ldapAuthenticate -> ldapAuthenticate.connectionOk },
    kafkaIsOk(adminClient, environment)
)

internal suspend fun PipelineContext<Unit, ApplicationCall>.respondOrServiceUnavailable(block: () -> Any) =
    try {
        val res = block()
        call.respond(res)
    } catch (e: Exception) {
        application.environment.log.error(EXCEPTION, e)
        val eMsg = when (e) {
            is java.util.concurrent.TimeoutException -> SERVICES_ERR_K
            else -> if (e.localizedMessage != null) e.localizedMessage else "exception occurred"
        }
        call.respond(HttpStatusCode.ServiceUnavailable, AnError(eMsg))
    }

internal suspend fun PipelineContext<Unit, ApplicationCall>.respondOrServiceUnavailable(
    environment: Environment,
    block: (lc: LDAPGroup) -> Any
) = try {
    LDAPGroup(environment).use { lc -> call.respond(block(lc)) }
} catch (e: Exception) {
    application.environment.log.error(EXCEPTION, e)
    call.respond(HttpStatusCode.ServiceUnavailable, AnError(e.localizedMessage))
}

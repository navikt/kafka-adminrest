package no.nav.integrasjon.api.v1

import io.ktor.application.ApplicationCall
import io.ktor.application.application
import io.ktor.application.call
import io.ktor.http.HttpStatusCode
import io.ktor.response.respond
import io.ktor.util.pipeline.PipelineContext
import mu.KotlinLogging
import no.nav.integrasjon.EXCEPTION
import no.nav.integrasjon.Environment
import no.nav.integrasjon.api.nais.client.SERVICES_ERR_K
import no.nav.integrasjon.ldap.LDAPAuthenticate
import no.nav.integrasjon.ldap.LDAPGroup
import org.apache.kafka.clients.admin.AdminClient
import java.util.concurrent.TimeUnit

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
const val CONSUMERGROUPS = "$API_V1/consumergroups"

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
            ?.get(environment.kafka.kafkaTimeout, TimeUnit.MILLISECONDS)?.isNotEmpty() ?: false
    } catch (e: Exception) {
        logger.error(e) { "Could not connect to kafka: timeout - ${environment.kafka.kafkaTimeout}" }
        false
    }

internal fun backEndServicesAreOk(
    adminClient: AdminClient?,
    environment: Environment
): Triple<Boolean, Boolean, Boolean> = Triple(
    try {
        LDAPGroup(environment).use { ldapGroup -> ldapGroup.connectionOk }
    } catch (e: Exception) {
        logger.error(e) { "LDAP group unavailable" }
        false
    },
    try {
        LDAPAuthenticate(environment).use { ldapAuthenticate -> ldapAuthenticate.connectionOk }
    } catch (e: Exception) {
        logger.error(e) { "LDAP authentication unavailable" }
        false
    },
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

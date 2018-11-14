package no.nav.integrasjon.api.v1

import io.ktor.application.ApplicationCall
import io.ktor.application.application
import io.ktor.application.call
import io.ktor.http.HttpStatusCode
import io.ktor.pipeline.PipelineContext
import io.ktor.response.respond
import no.nav.integrasjon.EXCEPTION
import no.nav.integrasjon.FasitProperties
import no.nav.integrasjon.api.nais.client.SERVICES_ERR_K
import no.nav.integrasjon.ldap.LDAPAuthenticate
import no.nav.integrasjon.ldap.LDAPGroup
import org.apache.kafka.clients.admin.AdminClient
import java.util.concurrent.TimeUnit

// nais api

internal const val NAIS_ISALIVE = "/isAlive"
internal const val NAIS_ISREADY = "/isReady"

internal const val KAFKA_TIMEOUT = 2_000L

// route starting point
internal const val API_V1 = "/api/v1"

// route for brokers and acls in kafka environment, and LDAP groups
internal const val BROKERS = "$API_V1/brokers"
internal const val ACLS = "$API_V1/acls"
internal const val GROUPS = "$API_V1/groups"

// route for topics in kafka environment, and zoom into related acls and groups per topic
internal const val TOPICS = "$API_V1/topics"
internal const val ONESHOT = "$API_V1/oneshot"

// simple data class for exceptions
internal data class AnError(val error: String)

internal fun kafkaIsOk(adminClient: AdminClient?): Boolean =
    try {
        adminClient
                ?.listTopics()
                ?.namesToListings()
                ?.get(KAFKA_TIMEOUT, TimeUnit.MILLISECONDS)?.isNotEmpty() ?: false
    } catch (e: Exception) { false }

internal fun backEndServicesAreOk(
    adminClient: AdminClient?,
    config: FasitProperties
): Triple<Boolean, Boolean, Boolean> =

    Triple(
            LDAPGroup(config).use { ldapGroup -> ldapGroup.connectionOk },
            LDAPAuthenticate(config).use { ldapAuthenticate -> ldapAuthenticate.connectionOk },
            kafkaIsOk(adminClient)
    )

internal suspend fun PipelineContext<Unit, ApplicationCall>.respondSelectiveCatch(block: () -> Pair<HttpStatusCode, Any>) =
        try {
            val res = block()
            call.respond(res.first, res.second)
        } catch (e: Exception) {
            application.environment.log.error(EXCEPTION, e)
            call.respond(HttpStatusCode.ExpectationFailed, AnError("$EXCEPTION$e"))
        }

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
    config: FasitProperties,
    block: (lc: LDAPGroup) -> Any
) =
        try { LDAPGroup(config).use { lc -> call.respond(block(lc)) }
        } catch (e: Exception) {
            application.environment.log.error(EXCEPTION, e)
            call.respond(HttpStatusCode.ServiceUnavailable, AnError(e.localizedMessage))
        }
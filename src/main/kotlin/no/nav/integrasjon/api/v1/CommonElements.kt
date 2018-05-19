package no.nav.integrasjon.api.v1

import io.ktor.application.ApplicationCall
import io.ktor.application.application
import io.ktor.application.call
import io.ktor.http.HttpStatusCode
import io.ktor.pipeline.PipelineContext
import io.ktor.response.respond

// path starting point
internal const val API_V1 = "/api/v1"

// path for brokers and acls in kafka environment, and all LDAP groups
internal const val BROKERS = "$API_V1/brokers"
internal const val ACLS = "$API_V1/acls"
internal const val GROUPS = "$API_V1/groups"

// path for topics in kafka environment, and zoom into related acls and groups per topic
internal const val TOPICS = "$API_V1/topics"

// simple data class for exceptions
internal data class AnError(val error: String)

// a wrapper for each call to AdminClient - used in routes
internal suspend fun PipelineContext<Unit, ApplicationCall>.respondAndCatch(block: () -> Any) =
        try {
            call.respond(block())
        }
        catch (e: Exception) {
            application.environment.log.error("Sorry, exception happened - $e")
            call.respond(HttpStatusCode.ExceptionFailed, AnError("Sorry, exception happened - $e"))
        }
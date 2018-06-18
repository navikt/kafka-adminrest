package no.nav.integrasjon.api.v1

import io.ktor.application.ApplicationCall
import io.ktor.application.application
import io.ktor.application.call
import io.ktor.http.HttpStatusCode
import io.ktor.pipeline.PipelineContext
import io.ktor.response.respond
import no.nav.integrasjon.EXCEPTION

// route starting point
internal const val API_V1 = "/api/v1"

// route for brokers and acls in kafka environment, and LDAP groups
internal const val BROKERS = "$API_V1/brokers"
internal const val ACLS = "$API_V1/acls"
internal const val GROUPS = "$API_V1/groups"

// route for topics in kafka environment, and zoom into related acls and groups per topic
internal const val TOPICS = "$API_V1/topics"

// simple data class for exceptions
internal data class AnError(val error: String)

// a wrapper for each call to AdminClient - used in routes
internal suspend fun PipelineContext<Unit, ApplicationCall>.respondCatch(block: () -> Any) =
        try {
            call.respond(block())
        } catch (e: Exception) {
            application.environment.log.error(EXCEPTION, e)
            call.respond(HttpStatusCode.ExceptionFailed, AnError("$EXCEPTION$e"))
        }

internal suspend fun PipelineContext<Unit, ApplicationCall>.respondSelectiveCatch(block: () -> Pair<HttpStatusCode, Any>) =
        try {
            val res = block()
            call.respond(res.first, res.second)
        } catch (e: Exception) {
            application.environment.log.error(EXCEPTION, e)
            call.respond(HttpStatusCode.ExceptionFailed, AnError("$EXCEPTION$e"))
        }
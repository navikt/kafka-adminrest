package no.nav.integrasjon.api.v1

import io.ktor.application.ApplicationCall
import io.ktor.application.application
import io.ktor.application.call
import io.ktor.http.HttpStatusCode
import io.ktor.pipeline.PipelineContext
import io.ktor.response.respond
import io.ktor.routing.*
import no.nav.integrasjon.EXCEPTION
import no.nav.integrasjon.FasitProperties
import no.nav.integrasjon.ldap.LDAPGroup

/**
 * Groups API
 * just a couple of read only routes
 * - get all kafka groups in LDAP
 * - get config. details for a specific broker
 */

// a wrapper for this api to be installed as routes
fun Routing.groupsAPI(config: FasitProperties) {

    getGroups(config)
    getGroupMembers(config)
}

// a wrapper for each call to ldap - used in routes
private suspend fun PipelineContext<Unit, ApplicationCall>.ldapRespondCatch(
        config: FasitProperties,
        block: (lc: LDAPGroup) -> Any) =
        try {
            LDAPGroup(config).use { lc ->
                call.respond(block(lc))
            }
        }
        catch (e: Exception) {
            application.environment.log.error(EXCEPTION, e)
            call.respond(HttpStatusCode.ExceptionFailed, AnError("$EXCEPTION$e"))
        }

/**
 * GET https://<host>/api/v1/groups
 *
 * See LDAPGroup::getKafkaGroups
 *
 * Returns a collection of String - group names
 */
fun Routing.getGroups(config: FasitProperties) =
        get(GROUPS) {
            ldapRespondCatch(config) { lc ->
                lc.getKafkaGroups()
            }
        }

/**
 * GET https://<host>/api/v1/groups/{groupName}
 *
 * See LDAP::getKafkaGroupMembers
 *
 * Returns a collection of String - members of given group
 */
fun Routing.getGroupMembers(config: FasitProperties) =
        get("$GROUPS/{groupName}") {
            ldapRespondCatch(config) { lc ->
                call.parameters["groupName"]?.let { groupName ->
                    lc.getKafkaGroupMembers(groupName)
                } ?: emptyList<String>()
            }
        }
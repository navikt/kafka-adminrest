package no.nav.integrasjon.api.v1

import io.ktor.application.ApplicationCall
import io.ktor.application.application
import io.ktor.application.call
import io.ktor.http.HttpStatusCode
import io.ktor.locations.Location
import io.ktor.pipeline.PipelineContext
import io.ktor.response.respond
import io.ktor.routing.Routing
import no.nav.integrasjon.EXCEPTION
import no.nav.integrasjon.FasitProperties
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.Group
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.failed
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.get
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.ok
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.responds
import no.nav.integrasjon.ldap.LDAPGroup

/**
 * Groups API
 * just a couple of read only routes
 * - get all kafka groups in LDAP
 * - get members of a specific group
 *
 * Observe that 'all' groups is those groups under FasitProperties::ldapGroupBase
 */

// a wrapper for this api to be installed as routes
fun Routing.groupsAPI(config: FasitProperties) {

    getGroups(config)
    getGroupMembers(config)
}

private const val swGroup = "Groups"

// a wrapper for each call to ldap - used in routes
private suspend fun PipelineContext<Unit, ApplicationCall>.ldapRespondCatch(
    config: FasitProperties,
    block: (lc: LDAPGroup) -> Any
) =
        try {
            LDAPGroup(config).use { lc ->
                call.respond(block(lc))
            }
        } catch (e: Exception) {
            application.environment.log.error(EXCEPTION, e)
            call.respond(HttpStatusCode.ExceptionFailed, AnError("$EXCEPTION$e"))
        }

/**
 * See LDAPGroup::getKafkaGroups
 */

@Group(swGroup)
@Location(GROUPS)
class GetGroups

data class GetGroupsModel(val groups: List<String>)

fun Routing.getGroups(config: FasitProperties) =
        get<GetGroups>("all groups".responds(ok<GetGroupsModel>(), failed<AnError>())) {
            ldapRespondCatch(config) { lc ->
                GetGroupsModel(lc.getKafkaGroups().toList())
            }
        }

/**
 * See LDAP::getKafkaGroupMembers
 */

@Group(swGroup)
@Location("$GROUPS/{groupName}")
data class GetGroupMembers(val groupName: String)

data class GetGroupMembersModel(val name: String, val members: List<String>)

fun Routing.getGroupMembers(config: FasitProperties) =
        get<GetGroupMembers>("members in a group".responds(ok<GetGroupMembersModel>(), failed<AnError>())) { group ->
            ldapRespondCatch(config) { lc ->
                GetGroupMembersModel(group.groupName, lc.getKafkaGroupMembers(group.groupName))
            }
        }

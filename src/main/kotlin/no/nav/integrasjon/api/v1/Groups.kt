package no.nav.integrasjon.api.v1

import io.ktor.locations.Location
import io.ktor.routing.Routing
import no.nav.integrasjon.Environment
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.Group
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.get
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.ok
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.responds
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.serviceUnavailable

/**
 * Groups API
 * just a couple of read only routes
 * - get all kafka groups in LDAP
 * - get members of a specific group
 *
 * Observe that 'all' groups is those groups under FasitProperties::ldapGroupBase
 */

// a wrapper for this api to be installed as routes
fun Routing.groupsAPI(environment: Environment) {

    getGroups(environment)
    getGroupMembers(environment)
}

private const val swGroup = "Groups"

/**
 * See LDAPGroup::getKafkaGroups
 */

@Group(swGroup)
@Location(GROUPS)
class GetGroups

data class GetGroupsModel(val groups: List<String>)

fun Routing.getGroups(environment: Environment) =
        get<GetGroups>("all groups".responds(ok<GetGroupsModel>(), serviceUnavailable<AnError>())) {
            respondOrServiceUnavailable(environment) { lc -> GetGroupsModel(lc.getKafkaGroups().toList()) }
        }

/**
 * See LDAP::getKafkaGroupMembers
 */

@Group(swGroup)
@Location("$GROUPS/{groupName}")
data class GetGroupMembers(val groupName: String)

data class GetGroupMembersModel(val name: String, val members: List<String>)

fun Routing.getGroupMembers(environment: Environment) =
        get<GetGroupMembers>(
                "members in a group".responds(ok<GetGroupMembersModel>(),
                        serviceUnavailable<AnError>())
        ) { group ->
            respondOrServiceUnavailable(environment) { lc ->
                GetGroupMembersModel(group.groupName, lc.getKafkaGroupMembers(group.groupName))
            }
        }

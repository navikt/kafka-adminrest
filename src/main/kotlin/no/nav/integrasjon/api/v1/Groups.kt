package no.nav.integrasjon.api.v1

import io.ktor.locations.Location
import io.ktor.routing.Routing
import no.nav.integrasjon.FasitProperties
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
fun Routing.groupsAPI(fasitConfig: FasitProperties) {

    getGroups(fasitConfig)
    getGroupMembers(fasitConfig)
}

private const val swGroup = "Groups"

/**
 * See LDAPGroup::getKafkaGroups
 */

@Group(swGroup)
@Location(GROUPS)
class GetGroups

data class GetGroupsModel(val groups: List<String>)

fun Routing.getGroups(fasitConfig: FasitProperties) =
    get<GetGroups>("all groups".responds(ok<GetGroupsModel>(), serviceUnavailable<AnError>())) {
        respondOrServiceUnavailable(fasitConfig) { lc -> GetGroupsModel(lc.getKafkaGroups().toList()) }
    }

/**
 * See LDAP::getKafkaGroupMembers
 */

@Group(swGroup)
@Location("$GROUPS/{groupName}")
data class GetGroupMembers(val groupName: String)

data class GetGroupMembersModel(val name: String, val members: List<String>)

data class GetGroupInGroupMembersModel(
    val groupInGroupName: List<String> = emptyList(),
    val members: List<String> = emptyList()
)

data class GetGroupMembersModelResponse(val kafkaGroup: GetGroupMembersModel, val aDGroup: GetGroupInGroupMembersModel)

fun Routing.getGroupMembers(fasitConfig: FasitProperties) =
    get<GetGroupMembers>(
        "members in a group".responds(
            ok<GetGroupMembersModelResponse>(),
            serviceUnavailable<AnError>()
        )
    ) { group ->
        respondOrServiceUnavailable(fasitConfig) { lc ->

            val groupInGroupName = lc.memberIsGroup(group.groupName)
            val kafkaGroupAndMembers = GetGroupMembersModel(group.groupName, lc.getKafkaGroupMembers(group.groupName))
            when {
                groupInGroupName.isEmpty() -> GetGroupMembersModelResponse(
                    kafkaGroup = kafkaGroupAndMembers, aDGroup = GetGroupInGroupMembersModel()
                )
                else -> {
                    val allMembers = groupInGroupName.flatMap {
                        lc.getGroupInGroupMembers(it)
                    }
                    GetGroupMembersModelResponse(
                        kafkaGroup = kafkaGroupAndMembers,
                        aDGroup = GetGroupInGroupMembersModel(groupInGroupName, members = allMembers)
                    )
                }
            }
        }
    }

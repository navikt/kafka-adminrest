package no.nav.integrasjon.api.v1

import io.ktor.application.application
import io.ktor.application.call
import io.ktor.auth.UserIdPrincipal
import io.ktor.auth.authentication
import io.ktor.auth.principal
import io.ktor.http.HttpStatusCode
import io.ktor.locations.Location
import io.ktor.response.respond
import io.ktor.routing.Routing
import no.nav.integrasjon.FasitProperties
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.BasicAuthSecurity
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.Group
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.badRequest
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.get
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.ok
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.put
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.responds
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.securityAndReponds
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.serviceUnavailable
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.unAuthorized
import no.nav.integrasjon.ldap.GroupMemberOperation
import no.nav.integrasjon.ldap.LDAPGroup

fun Routing.apigwAPI(fasitConfig: FasitProperties) {

    getAllowedUsersInApiGwGroup(fasitConfig)
    updateApiGwGroup(fasitConfig)
}

private const val swGroup = "ApiGW"
private const val apiGw = "apigw"

@Group(swGroup)
@Location(APIGW)
class GetApiGatewayGroup

data class GetApiGwGroupMembersModel(val name: String, val members: List<String>)

fun Routing.getAllowedUsersInApiGwGroup(fasitConfig: FasitProperties) =
    get<GetApiGatewayGroup>("members in apigw group".responds(
        ok<GetApiGwGroupMembersModel>(),
        serviceUnavailable<AnError>())
    ) {
        respondOrServiceUnavailable(fasitConfig) { lc ->
            GetApiGwGroupMembersModel(apiGw, lc.getGroupMembers(apiGw))
        }
    }

@Group(swGroup)
@Location(APIGW)
class PutApiGatewayMember

enum class AdminOfApiGwGroup(val user: String) {
    ADMIN01("M151886")
}

data class ApiGwRequest(
    val members: List<ApiGwGroupMember>
)

data class ApiGwGroupMember(
    val memberName: String,
    val operation: GroupMemberOperation
)

data class ApiGwResult(
    val memberGroup: String,
    val action: ApiGwRequest
)

fun Routing.updateApiGwGroup(fasitConfig: FasitProperties) =
    put<PutApiGatewayMember, ApiGwRequest>(
        "add/remove members in apigw group. Only members defined as Admin are authorized".securityAndReponds(
            BasicAuthSecurity(),
            ok<ApiGwResult>(),
            badRequest<AnError>(),
            unAuthorized<AnError>())
    ) { _, body ->

        // Get Information from param
        val currentUser = call.principal<UserIdPrincipal>()!!.name
        val apiGwGroup = apiGw

        val logEntry = "Group membership update request by " +
            "${this.context.authentication.principal} - $apiGwGroup "
        application.environment.log.info(logEntry)

        // Is current User an Admin?
        if (!AdminOfApiGwGroup.values().map { it.user }.contains(currentUser)) {
            val msg = "Authenticated user: $currentUser is not allowed to update $apiGwGroup automatically"
            application.environment.log.error(msg)
            call.respond(HttpStatusCode.Unauthorized, AnError(msg))
            return@put
        }

        LDAPGroup(fasitConfig).use { ldap ->
            // Group do not exist, in environment - create and add currentUser As first?
            val groups = ldap.getKafkaGroups()
            if (!groups.contains(apiGwGroup)) {
                ldap.createGroup(apiGwGroup, currentUser)
            }

            // Group is already in environment - add to group, get Group Members
            val groupMembers = ldap.getGroupMembers(apiGwGroup)

            // 1. Check request body for users to add
            // 2. Check if user is allowed to be in environment
            val usersToBeAdded = body.members.map { member ->
                body.members.map { member.operation }.filter { GroupMemberOperation.ADD == it }
                Pair(member.memberName, member.operation)
            }
                .filter { !groupMembers.contains(it.first) }

            usersToBeAdded
                .filterNot { ldap.userExists(it.first) }
                .any {
                    val msg = "Tried to remove the user $it. who doesn't exist in AD"
                    application.environment.log.error(msg)
                    call.respond(HttpStatusCode.BadRequest, AnError(msg))
                    return@put
                }

            // 1. Check request body for users to remove
            // 2. Check if user is allowed to be in environment
            val usersToBeRemoved = body.members.map { member ->
                body.members.map { member.operation }.filter { GroupMemberOperation.REMOVE == it }
                Pair(member.memberName, member.operation)
            }
                .filter { !groupMembers.contains(it.first) }

            usersToBeRemoved
                .filterNot { ldap.userExists(it.first) }
                .any {
                    val msg = "Tried to add the user $it. who doesn't exist in AD"
                    application.environment.log.error(msg)
                    call.respond(HttpStatusCode.BadRequest, AnError(msg))
                    return@put
                }

            // Filter users not in group
            if (!usersToBeAdded.isEmpty()) {
                val userList = usersToBeAdded.map { it.first }
                ldap.addToGroup(apiGwGroup, userList)
                application.environment.log.info("$apiGwGroup group got updated: added member(s): $userList")
            }

            if (!usersToBeRemoved.isEmpty()) {
                val userList = usersToBeRemoved.map { it.first }
                ldap.removeGroupMembers(apiGwGroup, userList)
                application.environment.log.info("$apiGwGroup group has been updated: removed member(s): $userList")
            }
            // OK Scenario
            call.respond(ApiGwResult(apiGwGroup, body))
        }
    }

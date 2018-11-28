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
    val result: ApiGwRequest
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

        val logEntry = "Group membership update request by " +
            "${this.context.authentication.principal} - $apiGw "
        application.environment.log.info(logEntry)

        // Is current User an Admin?
        if (!AdminOfApiGwGroup.values().map { it.user }.contains(currentUser)) {
            val msg = "Authenticated user: $currentUser is not allowed to update $apiGw automatically"
            application.environment.log.error(msg)
            call.respond(HttpStatusCode.Unauthorized, AnError(msg))
            return@put
        }

        LDAPGroup(fasitConfig).use { ldap ->
            // Group do not exist, in environment - create and add currentUser As first?
            val groups = ldap.getKafkaGroups()
            if (!groups.contains(apiGw)) {
                ldap.createGroup(apiGw, currentUser)
                application.environment.log.info("Created ldap Group: $apiGw")
            }

            // Group is already in environment - add to group, get Group Members
            val groupMembers = ldap.getGroupMembers(apiGw)
            application.environment.log.debug("Get ldap Group members in: $apiGw, members: $groupMembers")

            // 1. Check request body for users to add
            val usersToBeAdded = body.members
                .map { member ->
                    Pair(member.memberName, member.operation)
                }.filter { it.second == GroupMemberOperation.ADD }
            application.environment.log.debug("Users that will be added: $usersToBeAdded")

            // 2. Check if user is allowed to be in environment
            usersToBeAdded
                .filter { !groupMembers.contains(it.first) }
                .filterNot { ldap.userExists(it.first) }
                .any {
                    val msg = "Tried to remove the user $it. who doesn't exist in AD"
                    application.environment.log.error(msg)
                    call.respond(HttpStatusCode.BadRequest, AnError(msg))
                    return@put
                }

            // 1. Check request body for users to remove
            val usersToBeRemoved = body.members
                .map { member ->
                    Pair(member.memberName, member.operation)
                }.filter { GroupMemberOperation.REMOVE == it.second }
            application.environment.log.debug("Users that will be removed: $usersToBeRemoved")

            // 2. Check if user is allowed to be in environment
            usersToBeRemoved
                .filterNot { ldap.userExists(it.first) }
                .any {
                    val msg = "Tried to add the user $it. who doesn't exist in AD"
                    application.environment.log.error(msg)
                    call.respond(HttpStatusCode.BadRequest, AnError(msg))
                    return@put
                }

            if (!usersToBeAdded.isEmpty()) {
                val userResult = usersToBeAdded.map { it.first }
                ldap.addToGroup(apiGw, userResult)
                val msg = "$apiGw group got updated: added member(s): $userResult"
                application.environment.log.info(msg)
            }

            if (!usersToBeRemoved.isEmpty()) {
                val userResult = usersToBeRemoved.map { it.first }
                ldap.removeGroupMembers(apiGw, userResult)
                val msg = "$apiGw group has been updated: removed member(s): $userResult"
                application.environment.log.info(msg)
            }
            // OK Scenario
            call.respond(ApiGwResult(apiGw, body))
        }
    }

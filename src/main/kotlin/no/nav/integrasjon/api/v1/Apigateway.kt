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
import no.nav.integrasjon.Environment
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

fun Routing.apigwAPI(environment: Environment) {

    getAllowedUsersInApiGwGroup(environment)
    updateApiGwGroup(environment)
}

private const val swGroup = "Api-gateway"
private const val apiGw = "apigw"

@Group(swGroup)
@Location(APIGW)
class GetApiGatewayGroup

data class GetApiGwGroupMembersModel(val name: String, val members: List<String>)

fun Routing.getAllowedUsersInApiGwGroup(environment: Environment) =
    get<GetApiGatewayGroup>(
        "all members in $apiGw group".responds(
            ok<GetApiGwGroupMembersModel>(),
            serviceUnavailable<AnError>()
        )
    ) {
        respondOrServiceUnavailable(environment) { lc ->
            GetApiGwGroupMembersModel(apiGw, lc.getGroupMembers(apiGw))
        }
    }

@Group(swGroup)
@Location(APIGW)
class PutApiGatewayMember

enum class AdminOfApiGwGroup(val user: String) {
    ADMIN01("m151886"),
    ADMIN02("n145821"),
    ADMIN03("n151873"),
    ADMIN04("s151890"),
    ADMIN05("s150563"),
    ADMIN06("r150567")
}

data class ApiGwRequest(
    val members: List<ApiGwGroupMember>
)

data class ApiGwGroupMember(
    val member: String,
    val operation: GroupMemberOperation
)

data class PutApiGwResultModel(
    val group: String,
    val result: ApiGwRequest
)

fun Routing.updateApiGwGroup(environment: Environment) =
    put<PutApiGatewayMember, ApiGwRequest>(
        "add/remove members in apigw group. Request add/removal on Slack in #kafka".securityAndReponds(
            BasicAuthSecurity(),
            ok<PutApiGwResultModel>(),
            badRequest<AnError>(),
            unAuthorized<AnError>()
        )
    ) { _, body ->

        val currentUser = call.principal<UserIdPrincipal>()!!.name.lowercase()

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

        LDAPGroup(environment).use { ldap ->
            // Group do not exist, in environment
            val groups = ldap.getKafkaGroups()
            if (!groups.contains(apiGw)) {
                ldap.createGroup(apiGw)
                application.environment.log.info("Created ldap Group: $apiGw")
            }

            // Group is already in environment - add to group, get Group Members
            val groupMembers = ldap.getGroupMembers(apiGw)
            application.environment.log.debug("Get ldap Group member(s) in: $apiGw, member(s): $groupMembers")

            // 1. Check request body for users to add
            val usersToBeAdded = try {
                body.members.map { member ->
                    Pair(member.member, member.operation)
                }
                    .filter { it.second == GroupMemberOperation.ADD }
                    .filter { !groupMembers.contains(it.first) } // 2. Add only users not already in group
            } catch (e: Exception) {
                val msg = "Request body is not valid. Check swagger documentation and model"
                application.environment.log.error(msg)
                call.respond(HttpStatusCode.BadRequest, AnError(msg))
                return@put
            }
            application.environment.log.debug("Users that will be added: $usersToBeAdded")

            // 3. Check for srv user
            usersToBeAdded.map { it.first }.filterNot { it.startsWith("srv") }
                .any {
                    val msg = "Tried to add the user: $it. Who is not an system user"
                    application.environment.log.error(msg)
                    call.respond(HttpStatusCode.BadRequest, AnError(msg))
                    return@put
                }

            // 4. Check if user exists in environment
            usersToBeAdded.map { it.first }.filterNot { ldap.userExists(it) }
                .any {
                    val msg = "Tried to add the user: $it. Who does not exist in current AD environment"
                    application.environment.log.error(msg)
                    call.respond(HttpStatusCode.BadRequest, AnError(msg))
                    return@put
                }

            // 1. Check request body for users to remove
            val usersToBeRemoved = try {
                body.members.map { member ->
                    Pair(member.member, member.operation)
                }
                    .filter { GroupMemberOperation.REMOVE == it.second }
                    .filter { groupMembers.contains(it.first) }
            } catch (e: Exception) {
                val msg = "Request body is not valid. Check swagger documentation and model"
                application.environment.log.error(msg)
                call.respond(HttpStatusCode.BadRequest, AnError(msg))
                return@put
            }
            application.environment.log.debug("Users that will be removed: $usersToBeRemoved")

            if (usersToBeAdded.isNotEmpty()) {
                val userResult = usersToBeAdded.map { it.first }
                ldap.addToGroup(apiGw, userResult)
                val msg = "$apiGw group got updated: added member(s): $userResult"
                application.environment.log.info(msg)
            }

            if (usersToBeRemoved.isNotEmpty()) {
                val userResult = usersToBeRemoved.map { it.first }
                ldap.removeGroupMembers(apiGw, userResult)
                val msg = "$apiGw group got updated: removed member(s): $userResult"
                application.environment.log.info(msg)
            }
            // OK Scenario
            val logExit = "Group membership updated by " + "$currentUser in $apiGw " +
                "- Added: ${usersToBeAdded.toList()} - Removed: ${usersToBeRemoved.toList()}"
            application.environment.log.info(logExit)
            call.respond(PutApiGwResultModel(apiGw, body))
        }
    }

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
import no.nav.integrasjon.api.v1.APIGW
import no.nav.integrasjon.api.v1.AnError
import no.nav.integrasjon.api.v1.respondOrServiceUnavailable
import no.nav.integrasjon.ldap.GroupMemberOperation
import no.nav.integrasjon.ldap.LDAPGroup

fun Routing.apigwAPI(fasitConfig: FasitProperties) {

    getAllowedUsersInApiGwGroup(fasitConfig)
    updateApiGwGroup(fasitConfig)
}

private const val swGroup = "ApiGW"

enum class ApiGwGroup(val groupName: String) {
    APIGW_GROUP_NAME("apigw")
}

@Group(swGroup)
@Location(APIGW)

data class GetApiGwGroupMembersModel(val name: String, val members: List<String>)

fun Routing.getAllowedUsersInApiGwGroup(fasitConfig: FasitProperties) =
    get<ApiGwGroup>("members in apigw group".responds(
        ok<GetApiGwGroupMembersModel>(),
        serviceUnavailable<AnError>())
    ) { group ->
        respondOrServiceUnavailable(fasitConfig) { lc ->
            GetApiGwGroupMembersModel(group.groupName, lc.getGroupMembers(group.groupName))
        }
    }

@Group(swGroup)
@Location(APIGW)

enum class AdminOfApiGwGroup(val user: String) {
    ADMIN01("M151886"),
    ADMIN02("M141212")
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
    put<ApiGwGroup, ApiGwRequest>(
        "add/remove members in apigw group. Only members defined as Admin are authorized".securityAndReponds(
            BasicAuthSecurity(),
            ok<ApiGwResult>(),
            badRequest<AnError>(),
            unAuthorized<AnError>())
    ) { gwGroup, body ->

        // Get Information from param
        val currentUser = call.principal<UserIdPrincipal>()!!.name
        val apiGwGroup = gwGroup.groupName

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

        // Check request body for users to add
        val usersToBeAdded = body.members.map { member ->
            body.members
                .map {
                    member.operation
                }
                .filter { GroupMemberOperation.ADD == it }
            member.memberName
        }

        // Check request body for users to remove
        val usersToBeRemoved = body.members.map { member ->
            body.members
                .map {
                    member.operation
                }
                .filter { GroupMemberOperation.REMOVE == it }
            member.memberName
        }

        LDAPGroup(fasitConfig).use { ldap ->

            // Check if user is allowed to be in environment
            usersToBeAdded.map { it }
                .filterNot { ldap.userExists(it) }
                .any {
                    val msg = "Tried to add the user $it. who doesn't exist in AD"
                    application.environment.log.error(msg)
                    call.respond(HttpStatusCode.BadRequest, AnError(msg))
                    return@put
                }

            // Group do not exist, in environment - create and add currentUser As first?
            val groups = ldap.getKafkaGroups()
            if (!groups.contains(apiGwGroup)) {
                ldap.createGroup(apiGwGroup, currentUser)
            }

            // Group is already in environment - add to group, get Group Members
            val groupMembers = ldap.getGroupMembers(apiGwGroup)

            // Filter users not in group
            if (!usersToBeAdded.isEmpty()) {
                val userADDBase = usersToBeAdded
                    .map { it }
                    .filter { !groupMembers.contains(it) }
                ldap.addToGroup(apiGwGroup, userADDBase)
                application.environment.log.info("$apiGwGroup group has been updated with members: $userADDBase")

                if (!usersToBeRemoved.isEmpty()) {
                    val userREMOVEBase = usersToBeRemoved
                        .map { it }
                        .filter { ldap.userExists(it) }
                    ldap.removeGroupMembers(apiGwGroup, userREMOVEBase)
                    application.environment.log.info("$apiGwGroup group has been updated with members: $userREMOVEBase")
                }
                call.respond(ApiGwResult(apiGwGroup, body))
            }
        }
    }

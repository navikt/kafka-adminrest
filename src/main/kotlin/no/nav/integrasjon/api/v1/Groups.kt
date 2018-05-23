package no.nav.integrasjon.api.v1

import io.ktor.application.ApplicationCall
import io.ktor.application.application
import io.ktor.application.call
import io.ktor.http.HttpStatusCode
import io.ktor.pipeline.PipelineContext
//import io.ktor.request.receive
import io.ktor.response.respond
import io.ktor.routing.*
//import kotlinx.coroutines.experimental.runBlocking
import no.nav.integrasjon.FasitProperties
import no.nav.integrasjon.ldap.LDAPGroup

// a wrapper for ldap api to be installed as routes
fun Routing.groupsAPI(config: FasitProperties) {

    getGroups(config)
    //createGroups(config)
    //deleteGroup(config)

    getGroupMembers(config)
    //updateGroupMembers(config)
}

// a wrapper for each call to ldap - used in routes
private suspend fun PipelineContext<Unit, ApplicationCall>.ldap(config: FasitProperties, block: (lc: LDAPGroup) -> Any) =
        try {
            LDAPGroup(config).use { lc ->
                call.respond(block(lc))
            }
        }
        catch (e: Exception) {
            application.environment.log.error("Sorry, exception happened - $e")
            call.respond(HttpStatusCode.ExceptionFailed, AnError("Sorry, exception happened - $e"))
        }

fun Routing.getGroups(config: FasitProperties) = get(GROUPS) { ldap(config) { lc -> lc.getKafkaGroups() } }

/*data class ManageGroup(val topicName: String)

fun Routing.createGroups(config: FasitProperties) =
        post("$GROUPS") {
            ldap(config) { lc ->

                val group = runBlocking { call.receive<ManageGroup>() }
                lc.createKafkaGroups(group.topicName)
            } }*/

/*fun Routing.deleteGroup(config: FasitProperties) =
        delete("$GROUPS/{groupName}") {
            ldap(config) { lc ->
                call.parameters["groupName"]?.let { lc.deleteKafkaGroup(it) } ?: emptyList<String>()
            }
        }*/

fun Routing.getGroupMembers(config: FasitProperties) =
        get("$GROUPS/{groupName}") {
            ldap(config) { lc ->
            call.parameters["groupName"]?.let { lc.getKafkaGroupMembers(it) } ?: emptyList<String>()
            }
        }

/*fun Routing.updateGroupMembers(config: FasitProperties) =
        put("$GROUPS/{groupName}") {
            ldap(config) {lc ->

                val updateEntry = runBlocking { call.receive<LDAPGroup.UpdateKafkaGroupMember>() }
                val groupName = call.parameters["groupName"] ?: "invalid"

                lc.updateKafkaGroupMembership(groupName, updateEntry)
            }
        }*/

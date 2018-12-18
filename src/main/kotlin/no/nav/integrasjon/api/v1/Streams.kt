package no.nav.integrasjon.api.v1

import io.ktor.application.call
import io.ktor.auth.UserIdPrincipal
import io.ktor.auth.principal
import io.ktor.http.HttpStatusCode
import io.ktor.locations.Location
import io.ktor.response.respond
import io.ktor.routing.Routing
import no.nav.integrasjon.FasitProperties
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.BasicAuthSecurity
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.Group
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.badRequest
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.ok
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.post
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.securityAndReponds
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.serviceUnavailable
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.unAuthorized
import no.nav.integrasjon.ldap.LDAPGroup
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.acl.AccessControlEntry
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.acl.AclPermissionType
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourceType
import java.util.concurrent.TimeUnit

private const val swGroup = "Streams"

enum class PostStreamStatus {
    ERROR,
    OK
}

@Group(swGroup)
@Location("$STREAMS/")
class PostStream

data class PostStreamBody(val applicationName: String, val user: String)

data class PostStreamResponse(val status: PostStreamStatus, val message: String)

fun Routing.streamsAPI(adminClient: AdminClient?, fasitConfig: FasitProperties) {
    post<PostStream, PostStreamBody>(
        "new streams app. The stream app will get permissions to create new internal topics."
            .securityAndReponds(
                BasicAuthSecurity(),
                ok<PostStreamResponse>(),
                serviceUnavailable<AnError>(),
                badRequest<AnError>(),
                unAuthorized<Unit>()
            )
    ) { _, body ->

        val currentUser = call.principal<UserIdPrincipal>()!!.name

        LDAPGroup(fasitConfig).use { ldap ->
            if (!ldap.userExists(currentUser)) {
                val err = "authenticated user $currentUser doesn't exist as NAV ident or service user in " +
                    "current LDAP domain, cannot register a stream name as prefix of topic"
                log.error(err)
                call.respond(HttpStatusCode.Unauthorized, PostStreamResponse(
                    status = PostStreamStatus.ERROR,
                    message = err
                ))
            }

            adminClient?.listTopics()?.names()?.get(fasitConfig.kafkaTimeout, TimeUnit.MILLISECONDS)?.filter {
                it.startsWith(body.applicationName)
            }?.firstOrNull()?.let {
                log.error("Trying to register a stream app which is a prefix of $it")
                call.respond(HttpStatusCode.BadRequest, PostStreamResponse(
                    status = PostStreamStatus.ERROR,
                    message = "Stream name is prefix of a topic"
                ))
            } ?: run {
                val streamsAcl = listOf(
                    AclBinding(
                        ResourcePattern(ResourceType.TOPIC, body.applicationName, PatternType.PREFIXED),
                        AccessControlEntry("User:${body.user}", "*", AclOperation.ALL, AclPermissionType.ALLOW)
                    ),
                    AclBinding(
                        ResourcePattern(ResourceType.GROUP, body.applicationName, PatternType.PREFIXED),
                        AccessControlEntry("User:${body.user}", "*", AclOperation.ALL, AclPermissionType.ALLOW)
                    )
                )

                try {
                    adminClient?.createAcls(streamsAcl)?.all()?.get(fasitConfig.kafkaTimeout, TimeUnit.MILLISECONDS)
                    log.info("Successfully updated acl for stream app ${body.applicationName}")
                    call.respond(PostStreamResponse(
                        status = PostStreamStatus.OK,
                        message = "Successfully updated ACL"
                    ))
                } catch (e: Exception) {
                    log.error("Exception caught while updating ACL for stream app ${body.applicationName}", e)
                    call.respond(HttpStatusCode.ServiceUnavailable, PostStreamResponse(
                        status = PostStreamStatus.ERROR,
                        message = "Failed to update ACL"
                    ))
                }
            }
        }
    }
}

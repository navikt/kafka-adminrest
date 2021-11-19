package no.nav.integrasjon.api.v1

import io.ktor.application.call
import io.ktor.http.HttpStatusCode
import io.ktor.locations.Location
import io.ktor.response.respond
import io.ktor.routing.Routing
import no.nav.integrasjon.Environment
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.BasicAuthSecurity
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.Group
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.badRequest
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.ok
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.post
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.securityAndReponds
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.serviceUnavailable
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.unAuthorized
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

fun Routing.streamsAPI(adminClient: AdminClient?, environment: Environment) {
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
        adminClient?.listTopics()?.names()?.get(environment.kafka.kafkaTimeout, TimeUnit.MILLISECONDS)?.firstOrNull {
            it.startsWith(body.applicationName)
        }?.let {
            log.error("Trying to register a stream app which is a prefix of $it")
            call.respond(
                HttpStatusCode.BadRequest,
                PostStreamResponse(
                    status = PostStreamStatus.ERROR,
                    message = "Stream name is prefix of a topic"
                )
            )
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
                adminClient?.createAcls(streamsAcl)?.all()?.get(environment.kafka.kafkaTimeout, TimeUnit.MILLISECONDS)
                log.info("Successfully updated acl for stream app ${body.applicationName}")
                call.respond(
                    PostStreamResponse(
                        status = PostStreamStatus.OK,
                        message = "Successfully updated ACL"
                    )
                )
            } catch (e: Exception) {
                log.error("Exception caught while updating ACL for stream app ${body.applicationName}", e)
                call.respond(
                    HttpStatusCode.ServiceUnavailable,
                    PostStreamResponse(
                        status = PostStreamStatus.ERROR,
                        message = "Failed to update ACL"
                    )
                )
            }
        }
    }
}

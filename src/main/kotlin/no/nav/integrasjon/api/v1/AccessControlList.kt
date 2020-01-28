package no.nav.integrasjon.api.v1

import io.ktor.application.application
import io.ktor.auth.authentication
import io.ktor.locations.Location
import io.ktor.routing.Routing
import java.util.concurrent.TimeUnit
import no.nav.integrasjon.Environment
import no.nav.integrasjon.api.nais.client.SERVICES_ERR_K
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.BasicAuthSecurity
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.Group
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.get
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.ok
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.securityAndReponds
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.serviceUnavailable
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.unAuthorized
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.acl.AclBindingFilter

/**
 * Access Control List (acl) API
 * just a read only route
 * - get all acls
 */

// a wrapper for this api to be installed as routes
fun Routing.aclAPI(adminClient: AdminClient?, environment: Environment) {

    getACLS(adminClient, environment)
}

private const val swGroup = "Access Control Lists"

/**
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/admin/AdminClient.html#describeAcls-org.apache.kafka.common.acl.AclBindingFilter-
 */

@Group(swGroup)
@Location(ACLS)
class XGetACL

data class XGetACLModel(val acls: List<AclBinding>)

fun Routing.getACLS(adminClient: AdminClient?, environment: Environment) =
    get<XGetACL>("all access control lists".securityAndReponds(
        BasicAuthSecurity(),
        ok<XGetACLModel>(),
        serviceUnavailable<AnError>(),
        unAuthorized<Unit>())) {
        respondOrServiceUnavailable {
            val logEntry = "All ACLS view request by ${this.context.authentication.principal}"
            application.environment.log.info(logEntry)

            val acls = adminClient
                ?.describeAcls(AclBindingFilter.ANY)
                ?.values()
                ?.get(environment.kafka.kafkaTimeout, TimeUnit.MILLISECONDS)
                ?.toList()
                ?: throw Exception(SERVICES_ERR_K)
            XGetACLModel(acls)
        }
    }

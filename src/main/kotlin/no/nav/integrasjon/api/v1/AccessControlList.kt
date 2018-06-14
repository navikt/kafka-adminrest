package no.nav.integrasjon.api.v1

import io.ktor.locations.Location
import io.ktor.routing.Routing
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.Group
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.get
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.ok
import no.nav.integrasjon.api.nielsfalk.ktor.swagger.responds
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.acl.AclBindingFilter

/**
 * Access Control List (acl) API
 * just a read only route
 * - get all acls
 */

// a wrapper for this api to be installed as routes
fun Routing.aclAPI(adminClient: AdminClient) {

    getACLS(adminClient)
}

/**
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/admin/AdminClient.html#describeAcls-org.apache.kafka.common.acl.AclBindingFilter-
 */

@Group("Access Control List")
@Location(ACLS)
class Acls

data class AclsModel(val acls: List<AclBinding>)

fun Routing.getACLS(adminClient: AdminClient) =
        get<Acls>("all access control lists".responds(ok<AclsModel>())) {
            respondCatch {
                adminClient.describeAcls(AclBindingFilter.ANY)
                        .values()
                        .get()
            }
        }
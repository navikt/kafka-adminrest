package no.nav.integrasjon.api.v1

import io.ktor.routing.Routing
import io.ktor.routing.get
import org.apache.kafka.clients.admin.AdminClient
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
 * GET https://<host>/api/v1/acls
 *
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/admin/AdminClient.html#describeAcls-org.apache.kafka.common.acl.AclBindingFilter-
 *
 * Returns a collection of org.apache.kafka.common.acl.AclBinding
 *
 * See https://kafka.apache.org/10/javadoc/org/apache/kafka/common/acl/AclBinding.html
 */
fun Routing.getACLS(adminClient: AdminClient) =
        get(ACLS) {
            respondCatch {
                adminClient.describeAcls(AclBindingFilter.ANY)
                        .values()
                        .get()
            }
        }

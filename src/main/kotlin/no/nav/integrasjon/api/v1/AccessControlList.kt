package no.nav.integrasjon.api.v1

import io.ktor.routing.Routing
import io.ktor.routing.get
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.acl.AclBindingFilter

fun Routing.aclAPI(adminClient: AdminClient) {

    getACLS(adminClient)
}

fun Routing.getACLS(adminClient: AdminClient) =
        get(ACLS) { respondAndCatch { adminClient.describeAcls(AclBindingFilter.ANY).values().get() } }


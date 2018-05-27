package no.nav.integrasjon.ldap

import com.unboundid.ldap.sdk.*
import mu.KotlinLogging
import no.nav.integrasjon.*

/**
 * LDAPAuthenticate provides only canUserAuthenticate by simple LDAP bind verification
 *
 * See https://docs.ldap.com/ldap-sdk/docs/javadoc/overview-summary.html
 */

class LDAPAuthenticate(private val config: FasitProperties) :
        LDAPBase(config.getConnectionInfo(LdapConnectionType.AUTHENTICATION)) {

    fun canUserAuthenticate(user: String, pwd: String): Boolean =
            if (!ldapConnection.isConnected)
                false
            else {
                val connInfo = config.getConnectionInfo(LdapConnectionType.AUTHENTICATION)
                val userDN = config.userDN(user)

                try {
                    (ldapConnection.bind(userDN, pwd).resultCode == ResultCode.SUCCESS).also {
                        if (it) log.info { "Successful bind of $userDN to $connInfo" }
                    }
                }
                catch(e: LDAPException) {
                    log.error { "$EXCEPTION cannot bind $userDN to $connInfo, ${e.diagnosticMessage}" }
                    false
                }
            }

    companion object {

        val log = KotlinLogging.logger {  }
    }
}

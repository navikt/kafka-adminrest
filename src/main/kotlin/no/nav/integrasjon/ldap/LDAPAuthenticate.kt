package no.nav.integrasjon.ldap

import com.unboundid.ldap.sdk.*
import mu.KotlinLogging
import no.nav.integrasjon.*

/**
 * LDAPAuthenticate - simple class for LDAP bind verification
 */

class LDAPAuthenticate(private val config: FasitProperties) :
        LDAPBase(config.getConnectionInfo(LdapConnectionType.AUTHENTICATION)) {

    fun canUserAuthenticate(user: String, pwd: String): Boolean =
            if (!ldapConnection.isConnected)
                false
            else {
                val connInfo = config.getConnectionInfo(LdapConnectionType.AUTHENTICATION)
                val userDN = config.userDN(user)
                log.debug { "Trying bind of $userDN to $connInfo" }

                try {
                    ldapConnection.bind(userDN, pwd).resultCode == ResultCode.SUCCESS
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

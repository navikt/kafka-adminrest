package no.nav.integrasjon.ldap

import com.unboundid.ldap.sdk.DN
import com.unboundid.ldap.sdk.LDAPException
import com.unboundid.ldap.sdk.ResultCode
import mu.KotlinLogging
import no.nav.integrasjon.FasitProperties
import no.nav.integrasjon.LdapConnectionType
import no.nav.integrasjon.getConnectionInfo
import no.nav.integrasjon.userDN

/**
 * LDAPAuthenticate provides only canUserAuthenticate by simple LDAP bind verification
 *
 * See https://docs.ldap.com/ldap-sdk/docs/javadoc/overview-summary.html
 */

class LDAPAuthenticate(private val config: FasitProperties) :
        LDAPBase(config.getConnectionInfo(LdapConnectionType.AUTHENTICATION)) {

    fun canUserAuthenticate(user: String, pwd: String): Boolean =
            if (!ldapConnection.isConnected) false
            else {
                // fold over resolved DNs, NAV ident or service accounts (normal + Basta)
                resolveDNs(user).fold(false) { acc, dn -> acc || authenticated(dn, pwd, acc) }.also {

                    val connInfo = config.getConnectionInfo(LdapConnectionType.AUTHENTICATION)

                    when (it) {
                        true -> log.info { "Successful bind of $user to $connInfo" }
                        false -> log.error { "Cannot bind $user to $connInfo" }
                    }
                }
            }

    // resolve DNs for both service accounts, including those created in Basta. The order of DNs according to user name
    private fun resolveDNs(user: String): List<String> = config.userDN(user).let {

        val rdns = DN(it).rdNs
        val dnPrefix = rdns[rdns.indices.first]
        val dnPostfix = "${rdns[rdns.indices.last - 1]},${rdns[rdns.indices.last]}"
        val srvAccounts = listOf("OU=ApplAccounts,OU=ServiceAccounts", "OU=ServiceAccounts")

        if (isNAVIdent(user)) listOf(it)
        else srvAccounts.map { "$dnPrefix,$it,$dnPostfix" }
    }

    private fun authenticated(dn: String, pwd: String, alreadyAuthenticated: Boolean): Boolean =
            if (alreadyAuthenticated) true
            else
                try { (ldapConnection.bind(dn, pwd).resultCode == ResultCode.SUCCESS) } catch (e: LDAPException) { false }

    companion object {

        val log = KotlinLogging.logger { }
    }
}

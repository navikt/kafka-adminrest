package no.nav.integrasjon.ldap

import com.google.gson.annotations.SerializedName
import com.unboundid.ldap.sdk.*
import com.unboundid.util.ssl.SSLUtil
import com.unboundid.util.ssl.TrustAllTrustManager
import mu.KotlinLogging
import no.nav.integrasjon.FasitProperties
import no.nav.integrasjon.groupDN
import no.nav.integrasjon.srvUserDN
import no.nav.integrasjon.userDN

/**
 * A base class for LDAP operations
 */

//TODO - need to parameterize the ldap details
class LDAPAuthenticate : AutoCloseable {

    private val connectOptions = LDAPConnectionOptions().apply {
        connectTimeoutMillis = 2_000
    }

    //NB! - TrustAllTrustManager is too trusty, but good enough when inside corporate inner zone
    private val ldapConnection = LDAPConnection(
            SSLUtil(TrustAllTrustManager()).createSSLSocketFactory(),
            connectOptions)

    init {
        // initialize LDAP connection
        val lgInfo = "(ldapgw.adeo.no,636)"

        try {
            ldapConnection.connect("ldapgw.adeo.no", 636)
            log.debug { "Successfully connected to $lgInfo" }
        }
        catch (e: LDAPException) {
            log.error { "Exception occurred, all LDAP operations will fail $lgInfo - $e" }
            ldapConnection.setDisconnectInfo(
                    DisconnectType.IO_ERROR,
                    "Exception when connecting to LDAPS $lgInfo", e)
        }
    }

    override fun close() {
        log.debug {"Closing ldap connection" }
        ldapConnection.close()
    }

    fun canUserAuthenticate(user: String, pwd: String): Boolean =
            if (!ldapConnection.isConnected)
                false
            else {
                val userDN = "cn=$user,ou=Users,ou=NAV,ou=BusinessUnits,dc=adeo,dc=no"
                log.debug { "Trying bind for $userDN and given password" }

                try {
                    ldapConnection.bind(userDN, pwd).resultCode == ResultCode.SUCCESS
                }
                catch(e: LDAPException) {
                    log.error { "Exception occurred during bind - ${e.diagnosticMessage}" }
                    false
                }
            }


    companion object {

        val log = KotlinLogging.logger {  }
    }
}

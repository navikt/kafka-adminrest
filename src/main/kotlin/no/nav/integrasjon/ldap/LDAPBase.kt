package no.nav.integrasjon.ldap

import com.unboundid.ldap.sdk.DisconnectType
import com.unboundid.ldap.sdk.Filter
import com.unboundid.ldap.sdk.LDAPConnection
import com.unboundid.ldap.sdk.LDAPConnectionOptions
import com.unboundid.ldap.sdk.LDAPException
import com.unboundid.ldap.sdk.ResultCode
import com.unboundid.ldap.sdk.SearchRequest
import com.unboundid.ldap.sdk.SearchScope
import com.unboundid.util.ssl.SSLUtil
import com.unboundid.util.ssl.TrustAllTrustManager
import mu.KotlinLogging
import no.nav.integrasjon.EXCEPTION

/**
 * LDAPBase provides minimum set of services for LDAP operations
 * - establish connection to LDAPS endpoint with proper resource management by AutoCloseable
 * - check if connected
 */

abstract class LDAPBase(private val connInfo: LDAPBase.Companion.ConnectionInfo) : AutoCloseable {

    private val connectOptions = LDAPConnectionOptions().apply {
        connectTimeoutMillis = connInfo.timeout
    }

    // NB! - TrustAllTrustManager is too trusty, but good enough when inside corporate inner zone
    protected val ldapConnection = LDAPConnection(
            SSLUtil(TrustAllTrustManager()).createSSLSocketFactory(),
            connectOptions)

    init {
        // initialize LDAP connection
        try {
            ldapConnection.connect(connInfo.host, connInfo.port)
            log.debug { "Successfully connected to $connInfo" }
        } catch (e: LDAPException) {
            log.error { "$EXCEPTION LDAP operations against $connInfo will fail - $e" }
            ldapConnection.setDisconnectInfo(
                    DisconnectType.IO_ERROR,
                    "$EXCEPTION when connecting to LDAPS $connInfo", e)
        }
    }

    val connectionOk = ldapConnection.isConnected

    override fun close() {
        log.debug { "Closing ldap connection $connInfo" }
        ldapConnection.close()
    }

    protected fun getDN(searchBase: String, attributeName: String): (String) -> String = { usrName ->
        ldapConnection.search(
                SearchRequest(
                        searchBase,
                        SearchScope.SUB,
                        Filter.createEqualityFilter(
                                attributeName,
                                usrName
                        ),
                        SearchRequest.NO_ATTRIBUTES
                )
        )
                .let { searchRes ->
                    when (searchRes.resultCode == ResultCode.SUCCESS && searchRes.entryCount == 1) {
                        true -> searchRes.searchEntries[0].dn
                        false -> ""
                    }
                }
    }

    companion object {

        data class ConnectionInfo(val host: String, val port: Int, val timeout: Int = 2_000)

        val log = KotlinLogging.logger { }
    }
}

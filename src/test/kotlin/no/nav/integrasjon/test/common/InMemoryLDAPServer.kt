package no.nav.integrasjon.test.common

import com.unboundid.ldap.listener.InMemoryDirectoryServer
import com.unboundid.ldap.listener.InMemoryDirectoryServerConfig
import com.unboundid.ldap.listener.InMemoryListenerConfig
import com.unboundid.ldap.sdk.OperationType
import com.unboundid.ldap.sdk.schema.Schema
import com.unboundid.util.ssl.KeyStoreKeyManager
import com.unboundid.util.ssl.SSLUtil
import com.unboundid.util.ssl.TrustAllTrustManager
import com.unboundid.util.ssl.TrustStoreTrustManager
import mu.KotlinLogging
import no.nav.integrasjon.EXCEPTION

/**
 * An object creating a in-memory LDAP server
 * - using LDAPS
 * - not allowing anonymous access to compare, thus, must bind first
 * - a baseDN that is enriched with resource/UserAndGroups.ldif
 * - start and stop functions to be used before/after test cases
 */

object InMemoryLDAPServer {

    const val LPORT = 11636
    private val log = KotlinLogging.logger { }
    private const val LNAME = "LDAPS"

    private val imConf = InMemoryDirectoryServerConfig("dc=test,dc=local").apply {

        try {

            val kStore = "src/test/resources/inmds.jks"
            val tlsCF = SSLUtil(TrustAllTrustManager()).createSSLSocketFactory()
            val tlsSF = SSLUtil(
                    KeyStoreKeyManager(kStore, "password".toCharArray(), "JKS", "inmds"),
                    TrustStoreTrustManager(kStore)
            ).createSSLServerSocketFactory()

            setListenerConfigs(
                    InMemoryListenerConfig.createLDAPSConfig(
                            LNAME,
                            null,
                            LPORT, tlsSF, tlsCF)
            )

            // require authentication for most operations except bind
            setAuthenticationRequiredOperationTypes(
                    OperationType.COMPARE,
                    OperationType.SEARCH,
                    OperationType.ADD,
                    OperationType.MODIFY,
                    OperationType.DELETE
                    )
            // let the embedded server use identical schema as apache DS configured for AD support (group and sAMAcc..)
            schema = Schema.getSchema("src/test/resources/apacheDS.ldif")
        } catch (e: Exception) { log.error { "$EXCEPTION$e" } }
    }

    private val imDS = InMemoryDirectoryServer(imConf).apply {
        try { importFromLDIF(true, "src/test/resources/UsersAndGroups.ldif") } catch (e: Exception) { log.error { "$EXCEPTION$e" } }
    }

    fun start() = imDS.startListening(LNAME)

    fun stop() = imDS.shutDown(true)
}

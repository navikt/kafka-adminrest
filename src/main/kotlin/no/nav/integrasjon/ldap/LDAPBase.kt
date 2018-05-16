package no.nav.integrasjon.ldap

import com.unboundid.ldap.sdk.*
import com.unboundid.util.ssl.SSLUtil
import com.unboundid.util.ssl.TrustAllTrustManager
import mu.KotlinLogging
import no.nav.integrasjon.FasitProperties
import no.nav.integrasjon.api.v1.ldap.Operation
import no.nav.integrasjon.api.v1.ldap.UpdateGroupMember
import no.nav.integrasjon.groupDN
import no.nav.integrasjon.srvUserDN
import no.nav.integrasjon.userDN

/**
 * A base class for LDAP operations
 */

class LDAPBase(private val config: FasitProperties) : AutoCloseable {

    private val connectOptions = LDAPConnectionOptions().apply {
        connectTimeoutMillis = config.ldapConnTimeout
    }

    //NB! - TrustAllTrustManager is too trusty, but good enough when inside corporate inner zone
    private val ldapConnection = LDAPConnection(
            SSLUtil(TrustAllTrustManager()).createSSLSocketFactory(),
            connectOptions)

    init {
        // initialize LDAP connection
        try {
            ldapConnection.connect(config.ldapHost, config.ldapPort)
            log.debug { "Successfully connected to (${config.ldapHost},${config.ldapPort})" }
        }
        catch (e: LDAPException) {
            log.error {"All LDAP operations will fail! " +
                    "Exception when connecting to (${config.ldapHost},${config.ldapPort}) - ${e.diagnosticMessage}" }
            ldapConnection.setDisconnectInfo(
                    DisconnectType.IO_ERROR,
                    "Exception when connecting to LDAP(${config.ldapHost},${config.ldapPort})", e)
        }

        // initialize bind with srvkafkaadadm user, for all operations except bind of users
        try {
            ldapConnection.bind(config.srvUserDN(config.ldapUser),config.ldapPassword)
            log.debug("Successfully bind to (${config.ldapHost},${config.ldapPort}) " +
                    "with ${config.srvUserDN(config.ldapUser)}")
        }
        catch (e: LDAPException) {
            log.error("Most LDAP operations will fail! " +
                    "Exception during bind of ${config.srvUserDN(config.ldapUser)} " +
                    "to (${config.ldapHost},${config.ldapPort}) - ${e.diagnosticMessage}")
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
                log.debug { "Trying bind for ${config.userDN(user)} and given password" }

                try {
                    ldapConnection.bind(config.userDN(user), pwd).resultCode == ResultCode.SUCCESS
                }
                catch(e: LDAPException) {
                    log.error { "Exception occurred during bind - ${e.diagnosticMessage}" }
                    false
                }
            }

    fun getKafkaGroups(): Collection<String> =
            ldapConnection
                    .search(
                            SearchRequest(
                                    config.ldapGroupBase,
                                    SearchScope.ONE,
                                    Filter.createEqualityFilter(
                                            "objectClass",
                                            "groupOfUniqueNames"),
                                    config.ldapGroupAttrName)
                    )
                    .searchEntries.map { it.getAttribute(config.ldapGroupAttrName).value}

    fun getKafkaGroupMembers(groupName: String): Collection<String> =
            ldapConnection
                    .search(
                            SearchRequest(
                                    config.ldapGroupBase,
                                    SearchScope.ONE,
                                    Filter.createEqualityFilter(
                                            config.ldapGroupAttrName,
                                            groupName),
                                    config.ldapGrpMemberAttrName)
                    )
                    .searchEntries.flatMap { it.getAttribute(config.ldapGrpMemberAttrName).values.toList() }

    fun updateKafkaGroupMembership(groupName: String, updateEntry: UpdateGroupMember): LDAPResult =
            ModifyRequest(
                    config.groupDN(groupName),
                    Modification(
                            if (updateEntry.operation == Operation.ADD)
                                ModificationType.ADD
                            else
                                ModificationType.DELETE,
                            config.ldapGrpMemberAttrName,
                            updateEntry.memberDN
                    )
            ).let { ldapConnection.modify(it) }

    companion object {

        val log = KotlinLogging.logger {  }
    }
}

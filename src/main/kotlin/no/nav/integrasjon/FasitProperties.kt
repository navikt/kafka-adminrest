package no.nav.integrasjon

/**
 * FasitPropFactory is a factory for fasit properties in order to separate test versus normal execution
 */
object FasitPropFactory {

    private var fasitProperties_: FasitProperties? = null
    val fasitProperties: FasitProperties
        get() {
            if (fasitProperties_ == null) fasitProperties_ = FasitProperties()
            return fasitProperties_ ?: throw AssertionError("FasitPropFactory, null for fasitProperties_!")
        }

    fun setFasitProperties(fp: FasitProperties) { fasitProperties_ = fp }
}

/**
 * FasitProperties is a data class hosting relevant fasit properties required by this application
 * In addition, some suitable extension functions
 */
data class FasitProperties(
    // kafka details
    val kafkaBrokers: String = System.getenv("KAFKA_BROKERS")?.toString() ?: "",
    val kafkaClientID: String = System.getenv("KAFKA_CLIENTID")?.toString() ?: "",
    val kafkaSecurity: String = System.getenv("KAFKA_SECURITY")?.toString()?.toUpperCase() ?: "",
    val kafkaSecProt: String = System.getenv("KAFKA_SECPROT")?.toString() ?: "",
    val kafkaSaslMec: String = System.getenv("KAFKA_SASLMEC")?.toString() ?: "",
    val kafkaUser: String = System.getenv("KAFKA_USER")?.toString() ?: "",
    val kafkaPassword: String = System.getenv("KAFKA_PASSWORD")?.toString() ?: "",
    val kafkaTimeout: Long = System.getenv("KAFKA_CONNTIMEOUT")?.toLong() ?: 3_000L,

    // common ldap details for both authentication and group management
    val ldapConnTimeout: Int = System.getenv("LDAP_CONNTIMEOUT")?.toInt() ?: 2_000,
    val ldapUserAttrName: String = System.getenv("LDAP_USERATTRNAME")?.toString() ?: "",

    // ldap authentication details - production LDAP
    val ldapAuthHost: String = System.getenv("LDAP_AUTH_HOST")?.toString() ?: "",
    val ldapAuthPort: Int = System.getenv("LDAP_AUTH_PORT")?.toInt() ?: 0,
    val ldapAuthUserBase: String = System.getenv("LDAP_AUTH_USERBASE")?.toString() ?: "",

    // ldap details for managing ldap groups - different LDAP servers (test, preprod, production)
    val ldapHost: String = System.getenv("LDAP_HOST")?.toString() ?: "",
    val ldapPort: Int = System.getenv("LDAP_PORT")?.toInt() ?: 0,

    val ldapSrvUserBase: String = System.getenv("LDAP_SRVUSERBASE")?.toString() ?: "",
    val ldapGroupBase: String = System.getenv("LDAP_GROUPBASE")?.toString() ?: "",
    val ldapGroupAttrName: String = System.getenv("LDAP_GROUPATTRNAME")?.toString() ?: "",
    val ldapGrpMemberAttrName: String = System.getenv("LDAP_GRPMEMBERATTRNAME")?.toString() ?: "",

    // ldap user and pwd with enough authorization for managing ldap groups
    val ldapUser: String = System.getenv("LDAP_USER")?.toString() ?: "",
    val ldapPassword: String = System.getenv("LDAP_PASSWORD")?.toString() ?: ""
)

// Checking that enough information is provided

fun FasitProperties.kafkaSecurityEnabled(): Boolean = kafkaSecurity == "TRUE"

fun FasitProperties.kafkaSecurityComplete(): Boolean =
        kafkaSecProt.isNotEmpty() && kafkaSaslMec.isNotEmpty() && kafkaUser.isNotEmpty() && kafkaPassword.isNotEmpty()

fun FasitProperties.ldapAuthenticationInfoComplete(): Boolean =
        ldapUserAttrName.isNotEmpty() && ldapAuthHost.isNotEmpty() && ldapAuthPort != 0 && ldapAuthUserBase.isNotEmpty()

fun FasitProperties.ldapGroupInfoComplete(): Boolean =
        ldapHost.isNotEmpty() && ldapPort != 0 && ldapSrvUserBase.isNotEmpty() && ldapGroupBase.isNotEmpty() &&
                ldapGroupAttrName.isNotEmpty() && ldapGrpMemberAttrName.isNotEmpty() && ldapUser.isNotEmpty() &&
                ldapPassword.isNotEmpty()

// Connection factory for which ldap in matter

// enum class LdapConnectionType { AUTHENTICATION, GROUP }

// fun FasitProperties.getConnectionInfo(connType: LdapConnectionType) =
//        when (connType) {
//            LdapConnectionType.AUTHENTICATION -> LDAPBase.Companion.ConnectionInfo(//
//                    ldapAuthHost, ldapAuthPort, ldapConnTimeout
//            )
//            LdapConnectionType.GROUP -> LDAPBase.Companion.ConnectionInfo(
//                    ldapHost, ldapPort, ldapConnTimeout
//            )
//        }

// Return diverse distinguished name types

fun FasitProperties.userDN(user: String) = "$ldapUserAttrName=$user,$ldapAuthUserBase"

fun FasitProperties.srvUserDN() = "$ldapUserAttrName=$ldapUser,$ldapSrvUserBase"

fun FasitProperties.groupDN(groupName: String) = "$ldapGroupAttrName=$groupName,$ldapGroupBase"

package no.nav.integrasjon

/**
 * FasitProperties is a data class hosting relevant fasit properties required by this application
 */
data class FasitProperties(
    val kafkaBrokers: String = System.getenv("KAFKA_BROKERS")?.toString() ?: "",
    val kafkaClientID: String = System.getenv("KAFKA_CLIENTID")?.toString() ?: "",
    val kafkaSecurity: String = System.getenv("KAFKA_SECURITY")?.toString()?.toUpperCase() ?: "",
    val kafkaSecProt: String = System.getenv("KAFKA_SECPROT")?.toString() ?: "",
    val kafkaSaslMec: String = System.getenv("KAFKA_SASLMEC")?.toString() ?: "",
    val kafkaUser: String = System.getenv("KAFKA_USER")?.toString() ?: "",
    val kafkaPassword: String = System.getenv("KAFKA_PASSWORD")?.toString() ?: "",

    val ldapHost: String = System.getenv("LDAP_HOST")?.toString() ?: "",
    val ldapPort: Int = System.getenv("LDAP_PORT")?.toInt() ?: 0,
    val ldapConnTimeout: Int = System.getenv("LDAP_CONNTIMEOUT")?.toInt() ?: 2_000,

    val ldapUserBase: String = System.getenv("LDAP_USERBASE")?.toString() ?: "",
    val ldapSrvUserBase: String = System.getenv("LDAP_SRVUSERBASE")?.toString() ?: "",
    val ldapGroupBase: String = System.getenv("LDAP_GROUPBASE")?.toString() ?: "",
    val ldapUserAttrName: String = System.getenv("LDAP_USERATTRNAME")?.toString() ?: "",
    val ldapGroupAttrName: String = System.getenv("LDAP_GROUPATTRNAME")?.toString() ?: "",
    val ldapGrpMemberAttrName: String = System.getenv("LDAP_GRPMEMBERATTRNAME")?.toString() ?: "",

    // ldap user and pwd for managing groups
    val ldapUser: String = System.getenv("LDAP_USER")?.toString() ?: "",
    val ldapPassword: String = System.getenv("LDAP_PASSWORD")?.toString() ?: ""
)

fun FasitProperties.isEmpty(): Boolean = kafkaBrokers.isEmpty() || kafkaClientID.isEmpty() ||
        kafkaSecurity.isEmpty() || ldapHost.isEmpty() || ldapPort == 0 || ldapUserBase.isEmpty() ||
        ldapSrvUserBase.isEmpty() || ldapGroupBase.isEmpty() || ldapUserAttrName.isEmpty() ||
        ldapGroupAttrName.isEmpty() || ldapGrpMemberAttrName.isEmpty() || ldapUser.isEmpty() || ldapPassword.isEmpty()

fun FasitProperties.kafkaSecurityEnabled(): Boolean = kafkaSecurity == "TRUE"

fun FasitProperties.kafkaSecurityComplete(): Boolean =
        kafkaSecProt.isNotEmpty() && kafkaSaslMec.isNotEmpty() && kafkaUser.isNotEmpty() && kafkaPassword.isNotEmpty()

fun FasitProperties.userDN(user: String) = "$ldapUserAttrName=$user,$ldapUserBase"

fun FasitProperties.srvUserDN(user: String) = "$ldapUserAttrName=$user,$ldapSrvUserBase"

fun FasitProperties.groupDN(groupName: String) = "$ldapGroupAttrName=$groupName,$ldapGroupBase"

package no.nav.integrasjon

import com.natpryce.konfig.ConfigurationProperties
import com.natpryce.konfig.EnvironmentVariables
import com.natpryce.konfig.Key
import com.natpryce.konfig.intType
import com.natpryce.konfig.longType
import com.natpryce.konfig.overriding
import com.natpryce.konfig.stringType
import java.io.File

private const val vaultApplicationPropertiesPath = "/var/run/secrets/nais.io/vault/application.properties"

private val config = if (System.getenv("APPLICATION_PROFILE") == "remote") {
    ConfigurationProperties.systemProperties() overriding
        EnvironmentVariables() overriding
        ConfigurationProperties.fromFile(File(vaultApplicationPropertiesPath)) overriding
        ConfigurationProperties.fromResource("application.properties")
} else {
    ConfigurationProperties.systemProperties() overriding
        EnvironmentVariables() overriding
        ConfigurationProperties.fromResource("application.properties")
}

data class Environment(
    val kafka: Kafka = Kafka(),
    val ldapCommon: LdapCommon = LdapCommon(),
    val ldapAuthenticate: LdapAuthenticate = LdapAuthenticate(),
    val ldapGroup: LdapGroup = LdapGroup(),
    val ldapUser: LdapUser = LdapUser(),
    val flags: Flags = Flags()
) {
    data class Kafka(
        val kafkaBrokers: String = config[Key("kafka.brokers", stringType)],
        val kafkaClientID: String = config[Key("kafka.clientid", stringType)],
        val kafkaSecurity: String = config[Key("kafka.security", stringType)],
        val kafkaSecProt: String = config[Key("kafka.secprot", stringType)],
        val kafkaSaslMec: String = config[Key("kafka.saslmec", stringType)],
        val kafkaUser: String = config[Key("kafka.user", stringType)],
        val kafkaPassword: String = config[Key("kafka.password", stringType)],
        val kafkaTimeout: Long = config[Key("kafka.conntimeout", longType)]
    ) {
        fun securityEnabled(): Boolean =
            kafkaSecurity == "TRUE"

        fun securityComplete(): Boolean =
            kafkaSecProt.isNotEmpty() &&
                kafkaSaslMec.isNotEmpty() &&
                kafkaUser.isNotEmpty() &&
                kafkaPassword.isNotEmpty()
    }

    data class LdapCommon(
        val ldapConnTimeout: Int = config[Key("ldap.conntimeout", intType)],
        val ldapUserAttrName: String = config[Key("ldap.userattrname", stringType)]
    )

    data class LdapAuthenticate(
        val ldapAuthHost: String = config[Key("ldap.auth.host", stringType)],
        val ldapAuthPort: Int = config[Key("ldap.auth.port", intType)],
        val ldapAuthUserBase: String = config[Key("ldap.auth.userbase", stringType)]
    ) {

        fun infoComplete(): Boolean =
            LdapCommon().ldapUserAttrName.isNotEmpty() &&
                ldapAuthHost.isNotEmpty() &&
                ldapAuthPort != 0 &&
                ldapAuthUserBase.isNotEmpty()
    }

    data class LdapGroup(
        val ldapHost: String = config[Key("ldap.host", stringType)],
        val ldapPort: Int = config[Key("ldap.port", intType)],
        val ldapSrvUserBase: String = config[Key("ldap.srvuserbase", stringType)],
        val ldapGroupBase: String = config[Key("ldap.groupbase", stringType)],
        val ldapGroupAttrName: String = config[Key("ldap.groupattrname", stringType)],
        val ldapGrpMemberAttrName: String = config[Key("ldap.grpmemberattrname", stringType)],
        val ldapGroupInGroupBase: String = config[Key("ldap_groupingroupbase", stringType)],
        val ldapGroupAttrType: String = config[Key("ldap_groupattrtype", stringType)]
    ) {
        fun infoComplete(): Boolean =
            ldapHost.isNotEmpty() &&
                ldapPort != 0 &&
                ldapSrvUserBase.isNotEmpty() &&
                ldapGroupBase.isNotEmpty() &&
                ldapGroupAttrName.isNotEmpty() &&
                ldapGrpMemberAttrName.isNotEmpty() &&
                ldapGroupInGroupBase.isNotEmpty() &&
                ldapGroupAttrType.isNotEmpty() &&
                LdapUser().ldapUser.isNotEmpty() &&
                LdapUser().ldapPassword.isNotEmpty()
    }

    data class LdapUser(
        val ldapUser: String = config[Key("ldap.user", stringType)],
        val ldapPassword: String = config[Key("ldap.password", stringType)]
    )

    data class Flags(
        val topicCreationEnabled: Boolean = System.getenv("TOPIC_CREATION_ENABLED") == "true"
    )

    // Return diverse distinguished name types
    fun userDN(user: String) =
        "${LdapCommon().ldapUserAttrName}=$user,${LdapAuthenticate().ldapAuthUserBase}"

    fun srvUserDN() =
        "${LdapCommon().ldapUserAttrName}=${LdapUser().ldapUser},${LdapGroup().ldapSrvUserBase}"

    fun groupDN(groupName: String) =
        "${LdapGroup().ldapGroupAttrName}=$groupName,${LdapGroup().ldapGroupBase}"
}

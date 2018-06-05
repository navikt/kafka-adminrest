package no.nav.integrasjon.ldap

import com.google.gson.annotations.SerializedName
import com.unboundid.ldap.sdk.Attribute
import com.unboundid.ldap.sdk.Filter
import com.unboundid.ldap.sdk.LDAPException
import com.unboundid.ldap.sdk.SearchRequest
import com.unboundid.ldap.sdk.SearchScope
import com.unboundid.ldap.sdk.LDAPResult
import com.unboundid.ldap.sdk.ResultCode
import com.unboundid.ldap.sdk.AddRequest
import com.unboundid.ldap.sdk.CompareRequest
import com.unboundid.ldap.sdk.DeleteRequest
import com.unboundid.ldap.sdk.ModifyRequest
import com.unboundid.ldap.sdk.Modification
import com.unboundid.ldap.sdk.ModificationType
import com.unboundid.ldap.sdk.DN
import mu.KotlinLogging
import no.nav.integrasjon.EXCEPTION
import no.nav.integrasjon.FasitProperties
import no.nav.integrasjon.LdapConnectionType
import no.nav.integrasjon.getConnectionInfo
import no.nav.integrasjon.srvUserDN
import no.nav.integrasjon.groupDN

/**
 * LDAPGroup provides services for LDAP group management
 * - creation of groups
 * - deletion of groups
 * - add or remove group members
 *
 * Group management is restricted to Kafka context
 * - producer - and consumer group per topic, restricted to FasitProperties::ldapGroupBase
 * - members of producer - or consumer group, restricted to service accounts, FasitProperties::ldapSrvUserBase
 *
 * See See https://docs.ldap.com/ldap-sdk/docs/javadoc/overview-summary.html
 *
 * Ok, this class can be divided into cleaner classes (pure LDAP group and Kafka context), but
 * laziness and good-enough is the strongest competitor so far
 */

class LDAPGroup(private val config: FasitProperties) :
        LDAPBase(config.getConnectionInfo(LdapConnectionType.GROUP)) {

    init {
        // initialize bind of user with enough authorization for group operations

        val connInfo = config.getConnectionInfo(LdapConnectionType.GROUP)
        val srvUserDN = config.srvUserDN()
        try {
            ldapConnection.bind(srvUserDN, config.ldapPassword)
            log.debug { "Successfully bind of $srvUserDN to $connInfo" }
        } catch (e: LDAPException) {
            log.error("$EXCEPTION LDAP operations will fail. Bind failure for $srvUserDN to $connInfo - $e")
            ldapConnection.close()
        }
    }

    // fixed set of attributes for group creation, more will be added - see createKafkaGroup
    private val newGroupAttr = listOf(
            Attribute(
                    "objectClass",
                    "group"),
            Attribute(
                    "description",
                    "Generated by kafka-admin-rest, see https://github.com/navikt/kafka-adminrest")
    )

    fun getKafkaGroups(): Collection<String> =
            ldapConnection
                    .search(
                            SearchRequest(
                                    config.ldapGroupBase,
                                    SearchScope.ONE,
                                    Filter.createEqualityFilter(
                                            "objectClass",
                                            "group"),
                                    config.ldapGroupAttrName)
                    )
                    .searchEntries.map { it.getAttribute(config.ldapGroupAttrName).value }

    private fun groupTypesCatch(
        topicName: String,
        membBlck: (gn: String) -> Collection<String>,
        resBlck: (gn: String) -> LDAPResult
    ): Collection<KafkaGroup> =

            KafkaGroupType.values().map { groupType ->
                val groupName = toGroupName(groupType.prefix, topicName)

                try {
                    KafkaGroup(
                            groupType,
                            groupName,
                            groupName.let(membBlck),
                            groupName.let(resBlck)
                    )
                } catch (e: LDAPException) {
                    log.error { "$EXCEPTION$e" }
                    KafkaGroup(groupType, groupName, emptyList(), e.toLDAPResult())
                }
            }

    fun createKafkaGroups(topicName: String): Collection<KafkaGroup> =
            groupTypesCatch(
                    topicName,
                    { emptyList() },
                    { groupName -> createKafkaGroup(groupName) }
            )

    fun deleteKafkaGroups(topicName: String): Collection<KafkaGroup> =
            groupTypesCatch(
                    topicName,
                    { emptyList() },
                    { groupName -> deleteKafkaGroup(groupName) }
            )

    fun getKafkaGroupsAndMembers(topicName: String): Collection<KafkaGroup> =
            groupTypesCatch(
                    topicName,
                    { groupName -> getKafkaGroupMembers(groupName) },
                    { LDAPResult(0, ResultCode.SUCCESS) }
            )

    private fun createKafkaGroup(groupName: String): LDAPResult =
            ldapConnection.add(
                    AddRequest(
                            DN(config.groupDN(groupName)),
                            newGroupAttr.toMutableList().apply {
                                add(Attribute("cn", groupName))
                                add(Attribute("sAMAccountName", groupName))
                            })
                            .also { req -> log.info { "Create group request: $req" } }
            )

    private fun deleteKafkaGroup(groupName: String): LDAPResult =
            ldapConnection.delete(
                    DeleteRequest(
                            DN(config.groupDN(groupName))
                    ).also { req -> log.info { "Delete group request: $req" } }
            )

    fun getKafkaGroupMembers(groupName: String): Collection<String> = getGroupMembers(groupName)

    fun updateKafkaGroupMembership(topicName: String, updateEntry: UpdateKafkaGroupMember): LDAPResult =

            getServiceUserDN(updateEntry.member).let { srvUserDN ->
                if (srvUserDN.isEmpty())
                    throw Exception("Cannot find ${updateEntry.member} under ${config.ldapSrvUserBase}")
                else
                    config.groupDN(toGroupName(updateEntry.role.prefix, topicName)).let { groupDN ->

                        val req = ModifyRequest(
                                groupDN,
                                Modification(
                                        when (updateEntry.operation) {
                                            GroupMemberOperation.ADD -> ModificationType.ADD
                                            GroupMemberOperation.REMOVE -> ModificationType.DELETE
                                        },
                                        config.ldapGrpMemberAttrName,
                                        srvUserDN
                                )
                        )

                        log.info { "Update group membership request: $req for $srvUserDN" }

                        if (updateEntry.isRedundant(srvUserDN, groupDN, toGroupName(updateEntry.role.prefix, topicName)))
                            LDAPResult(0, ResultCode.SUCCESS)
                        else
                            ldapConnection.modify(req)
                }
            }

    private fun UpdateKafkaGroupMember.isRedundant(userDN: String, groupDN: String, groupName: String): Boolean =
            when (this.operation) {
                // check whether groups is empty or not, otherwise exception from AD - no member value...
                GroupMemberOperation.ADD -> if (groupEmpty(groupName)) false else userInGroup(userDN, groupDN)
                GroupMemberOperation.REMOVE -> if (groupEmpty(groupName)) true else !userInGroup(userDN, groupDN)
            }

    private fun userInGroup(userDN: String, groupDN: String): Boolean =
            // careful, AD will raise exception if group is empty, thus, no member attribute issue
            ldapConnection
                    .compare(CompareRequest(groupDN, config.ldapGrpMemberAttrName, userDN))
                    .compareMatched()

    private fun getServiceUserDN(name: String): String =
            ldapConnection.search(
                    SearchRequest(
                            config.ldapSrvUserBase,
                            SearchScope.SUB,
                            Filter.createEqualityFilter(
                                    config.ldapUserAttrName,
                                    name
                            ),
                            SearchRequest.NO_ATTRIBUTES
                    )
            )
                    .let { searchRes ->
                        when (searchRes.resultCode == ResultCode.SUCCESS && searchRes.entryCount == 1) {
                            true -> searchRes.searchEntries[0].dn
                            false -> "".also {
                                log.error { "Could not find $name anywhere under ${config.ldapSrvUserBase}" } }
                        }
                    }

    private fun groupEmpty(groupName: String): Boolean = getGroupMembers(groupName).isEmpty()

    private fun getGroupMembers(groupName: String): Collection<String> =
            ldapConnection.search(
                    SearchRequest(
                            config.ldapGroupBase,
                            SearchScope.ONE,
                            Filter.createEqualityFilter(
                                    config.ldapGroupAttrName,
                                    groupName
                            ),
                            config.ldapGrpMemberAttrName
                    )
            )
                    .searchEntries
                    .flatMap {
                        it.getAttribute(config.ldapGrpMemberAttrName)?.values?.toList() ?: emptyList()
                    }

    companion object {

        val log = KotlinLogging.logger { }

        private fun toGroupName(prefix: String, topicName: String) = "$prefix$topicName"

        /**
         * Ref. https://social.technet.microsoft.com/Forums/windows/en-US/0d7c1a2d-2bbe-4a54-9d1a-c3cff1871ed6/active-directory-group-name-character-limit?forum=winserverDS
         * The longest CommonName (CN) is limited to 64 characters
         * Kafka topic name length must be ≤ 246
         * The topic name is more restrictice than group name, check only the final group name length
         */
        private const val MAX_GROUPNAME_LENGTH = 64
        fun validGroupLength(topicName: String): Boolean =
                KafkaGroupType.values().map { it.prefix.length }.max()!! + topicName.length <= MAX_GROUPNAME_LENGTH

        fun maxTopicNameLength(): Int = MAX_GROUPNAME_LENGTH - KafkaGroupType.values().map { it.prefix.length }.max()!!

        /**
         * Enum class KafkaGroupType with LDAP group prefix included
         * Each topic has 2 groups
         * - a producer group with members allowed to produce events to topic
         * - a consumer group with members allowed to consume events from topic
         */
        enum class KafkaGroupType(val prefix: String) {
            @SerializedName("producer") PRODUCER("KP-"),
            @SerializedName("consumer") CONSUMER("KC-")
        }

        /**
         * Enum class KafkaGroupOperation
         * ADD - add a new group member
         * REMOVE - remove a group member from group
         */
        enum class GroupMemberOperation {
            @SerializedName("add") ADD,
            @SerializedName("remove") REMOVE
        }

        /**
         * data class UpdateKafkaGroupMember
         */
        data class UpdateKafkaGroupMember(
            val role: KafkaGroupType,
            val operation: GroupMemberOperation,
            val member: String
        )

        /**
         * data class KafkaGroup as result from functions iterating KafkaGroupType - see groupTypesCatch
         */
        data class KafkaGroup(
            val groupType: KafkaGroupType,
            val name: String,
            val members: Collection<String>,
            val result: LDAPResult
        )
    }
}

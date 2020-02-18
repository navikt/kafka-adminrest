package no.nav.integrasjon.ldap

import com.google.gson.annotations.SerializedName
import com.unboundid.asn1.ASN1OctetString
import com.unboundid.ldap.sdk.AddRequest
import com.unboundid.ldap.sdk.Attribute
import com.unboundid.ldap.sdk.CompareRequest
import com.unboundid.ldap.sdk.DN
import com.unboundid.ldap.sdk.DeleteRequest
import com.unboundid.ldap.sdk.Filter
import com.unboundid.ldap.sdk.LDAPException
import com.unboundid.ldap.sdk.LDAPResult
import com.unboundid.ldap.sdk.Modification
import com.unboundid.ldap.sdk.ModificationType
import com.unboundid.ldap.sdk.ModifyRequest
import com.unboundid.ldap.sdk.ResultCode
import com.unboundid.ldap.sdk.SearchRequest
import com.unboundid.ldap.sdk.SearchResult
import com.unboundid.ldap.sdk.SearchResultEntry
import com.unboundid.ldap.sdk.SearchScope
import com.unboundid.ldap.sdk.controls.SimplePagedResultsControl
import com.unboundid.util.LDAPTestUtils
import mu.KotlinLogging
import no.nav.integrasjon.EXCEPTION
import no.nav.integrasjon.Environment
import no.nav.integrasjon.getAuthenticationConnectionInfo
import no.nav.integrasjon.ldap.LDAPGroup.Companion.simplify
import org.apache.kafka.common.acl.AccessControlEntry
import org.apache.kafka.common.acl.AclBinding
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.acl.AclPermissionType
import org.apache.kafka.common.resource.PatternType
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourceType

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

fun toGroupName(prefix: String, topicName: String) = "$prefix$topicName"

class LDAPGroup(private val env: Environment) :
    LDAPBase(
        getAuthenticationConnectionInfo(
            env.ldapGroup.ldapHost,
            env.ldapGroup.ldapPort,
            env.ldapCommon.ldapConnTimeout
        )
    ) {

    init {
        // initialize bind of user with enough authorization for group operations

        val connInfo = getAuthenticationConnectionInfo(
            env.ldapGroup.ldapHost,
            env.ldapGroup.ldapPort,
            env.ldapCommon.ldapConnTimeout
        )
        val srvUserDN = env.srvUserDN()
        try {
            ldapConnection.bind(srvUserDN, env.ldapUser.ldapPassword)
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
            "group"
        ),
        Attribute(
            "description",
            "Generated by kafka-admin-rest, see https://github.com/navikt/kafka-adminrest"
        )
    )

    fun getKafkaGroups() = getKafkaGroupNames()

    /**
     * Generic function iterating group types and performing a couple of operations
     */

    private fun groupTypesCatch(
        existingGroups: List<String>,
        topicName: String,
        membBlck: (exists: Boolean, gn: String) -> List<String>,
        resBlck: (exists: Boolean, gn: String) -> SLDAPResult
    ): List<KafkaGroup> =

        KafkaGroupType.values().map { groupType ->
            val groupName = toGroupName(groupType.prefix, topicName)
            val groupExists = groupName in existingGroups

            try {
                KafkaGroup(
                    groupType,
                    groupName,
                    membBlck(groupExists, groupName),
                    resBlck(groupExists, groupName)
                )
            } catch (e: LDAPException) {
                log.error(e) { "$EXCEPTION$e" }
                KafkaGroup(groupType, groupName, emptyList(), e.toLDAPResult().simplify())
            }
        }

    fun createKafkaGroups(topicName: String, creator: String): List<KafkaGroup> =
        groupTypesCatch(
            getKafkaGroupNames(),
            topicName,
            { _, _ -> emptyList() },
            { exists, groupName -> createKafkaGroup(exists, groupName, creator) }
        )

    fun deleteKafkaGroups(topicName: String): List<KafkaGroup> =
        groupTypesCatch(
            getKafkaGroupNames(),
            topicName,
            { _, _ -> emptyList() },
            { exists, groupName -> deleteKafkaGroup(exists, groupName) }
        )

    fun getKafkaGroupsAndMembers(topicName: String): List<KafkaGroup> =
        groupTypesCatch(
            getKafkaGroupNames(),
            topicName,
            { exists, groupName -> getMembersInKafkaGroup(groupName, exists) },
            { exists, _ ->
                if (exists)
                    LDAPResult(ResultCode.SUCCESS_INT_VALUE, ResultCode.SUCCESS).simplify()
                else
                    LDAPResult(ResultCode.NO_SUCH_OBJECT_INT_VALUE, ResultCode.NO_SUCH_OBJECT).simplify()
            }
        )

    fun createGroup(groupName: String, initialMember: String? = null) {
        ldapConnection.add(AddRequest(DN(env.groupDN(groupName)), newGroupAttr.toMutableList().apply {
            add(Attribute("cn", groupName))
            add(Attribute("sAMAccountName", groupName))
            if (initialMember != null) {
                add(Attribute(env.ldapGroup.ldapGrpMemberAttrName, resolveUserDN(initialMember)))
            }
        }))
    }

    fun removeGroupMembers(groupName: String, membersToRemove: List<String>) {
        ldapConnection.modify(env.groupDN(groupName), membersToRemove.map {
            Modification(ModificationType.DELETE, env.ldapGroup.ldapGrpMemberAttrName, resolveUserDN(it))
        })
    }

    fun addToGroup(groupName: String, groupMembers: List<String>) {
        ldapConnection.modify(env.groupDN(groupName), groupMembers.map {
            Modification(ModificationType.ADD, env.ldapGroup.ldapGrpMemberAttrName, resolveUserDN(it))
        })
    }

    fun getGroupMembers(groupName: String): List<String> =
        searchGetMembershipKN(Filter.createEqualityFilter(env.ldapGroup.ldapGroupAttrName, groupName))
            .flatMap {
                (it.getAttribute(env.ldapGroup.ldapGrpMemberAttrName)?.values?.toList() ?: listOf<String>())
                    .map { attribute ->
                        DN(attribute).rdn.attributes.first { first ->
                            first.name.equals(env.ldapCommon.ldapUserAttrName, ignoreCase = true)
                        }.value
                    }
            }

    private fun createKafkaGroup(exists: Boolean, groupName: String, creator: String): SLDAPResult =
        if (exists)
            LDAPResult(ResultCode.ENTRY_ALREADY_EXISTS_INT_VALUE, ResultCode.ENTRY_ALREADY_EXISTS).simplify()
        else
            ldapConnection.add(
                AddRequest(
                    DN(env.groupDN(groupName)),
                    newGroupAttr.toMutableList().apply {
                        add(Attribute("cn", groupName))
                        add(Attribute("sAMAccountName", groupName))

                        resolveUserDN(creator).let { userDN ->
                            if (groupName.startsWith(KafkaGroupType.MANAGER.prefix) &&
                                userDN.isNotEmpty())
                                add(Attribute(env.ldapGroup.ldapGrpMemberAttrName, userDN))
                        }
                    })
                    .also { req -> log.info { "Create group request: $req" } }
            ).simplify()

    private fun deleteKafkaGroup(exists: Boolean, groupName: String): SLDAPResult =
        if (!exists)
            LDAPResult(ResultCode.NO_SUCH_OBJECT_INT_VALUE, ResultCode.NO_SUCH_OBJECT).simplify()
        else
            ldapConnection.delete(
                DeleteRequest(
                    DN(env.groupDN(groupName))
                ).also { req -> log.info { "Delete group request: $req" } }
            ).simplify()

    fun getKafkaGroupMembers(groupName: String) = getMembersInKafkaGroup(groupName)

    fun updateKafkaGroupMembership(topicName: String, updateEntry: UpdateKafkaGroupMember): SLDAPResult =
        resolveUserDN(updateEntry.member).let { userDN ->
            if (userDN.isEmpty())
                throw Exception("Cannot find ${updateEntry.member} under user - or service accounts")
            else if (updateEntry.role != KafkaGroupType.MANAGER && isNAVIdent(updateEntry.member))
                throw Exception("Cannot have ${updateEntry.member} as consumer/producer")
            else
                env.groupDN(toGroupName(updateEntry.role.prefix, topicName)).let { groupDN ->

                    val req = ModifyRequest(
                        groupDN,
                        Modification(
                            when (updateEntry.operation) {
                                GroupMemberOperation.ADD -> ModificationType.ADD
                                GroupMemberOperation.REMOVE -> ModificationType.DELETE
                            },
                            env.ldapGroup.ldapGrpMemberAttrName,
                            userDN
                        )
                    )

                    log.info { "Update group membership request: $req for $userDN" }

                    if (updateEntry.isRedundant(userDN, groupDN, toGroupName(updateEntry.role.prefix, topicName)))
                        LDAPResult(0, ResultCode.SUCCESS).simplify()
                    else
                        ldapConnection.modify(req).simplify()
                }
        }

    private fun UpdateKafkaGroupMember.isRedundant(userDN: String, groupDN: String, groupName: String): Boolean =
        when (this.operation) {
            GroupMemberOperation.ADD -> userInGroup(userDN, groupDN, groupName)
            GroupMemberOperation.REMOVE -> !userInGroup(userDN, groupDN, groupName)
        }

    private fun userInGroup(userDN: String, groupDN: String, groupName: String): Boolean =
        // careful, AD will raise exception if group is empty, thus, no member attribute issue
        if (groupEmpty(groupName)) false
        else ldapConnection
            .compare(CompareRequest(groupDN, env.ldapGroup.ldapGrpMemberAttrName, userDN))
            .compareMatched()

    fun userIsManager(topicName: String, userName: String): Boolean =
        toGroupName(KafkaGroupType.MANAGER.prefix, topicName).let { groupName ->

            if (groupName in getKafkaGroupNames())
                userInGroup(
                    resolveUserDN(userName),
                    env.groupDN(toGroupName(KafkaGroupType.MANAGER.prefix, topicName)),
                    groupName)
            else false
        }

    fun userExists(userName: String): Boolean = resolveUserDN(userName).isNotEmpty().also { exists ->
        if (!exists) log.error { "$userName doesn't exists as NAV ident or service user in current LDAP domain" }
    }

    /**
     * Level 0 - Generic search function, find something somewhere in LDAP
     */

    private fun searchXInY(
        searchBase: String,
        searchScope: SearchScope
    ): (String) -> (Filter) -> SearchResult = { attribute ->
        { filter ->
            ldapConnection.search(SearchRequest(searchBase, searchScope, filter, attribute))
        }
    }

    private fun searchWithPagination(
        searchBase: String,
        searchScope: SearchScope
    ): (String) -> (Filter) -> List<SearchResultEntry> = { attribute ->
        { filter ->
            val searchRequest: SearchRequest = SearchRequest(searchBase, searchScope, filter, attribute)
            var resumeCookie: ASN1OctetString? = ASN1OctetString()
            val searchResultEntries: MutableList<SearchResultEntry> = mutableListOf()
            while (resumeCookie != null) {
                searchRequest.setControls(SimplePagedResultsControl(1000, resumeCookie))
                val searchResult: SearchResult = ldapConnection.search(searchRequest)
                searchResultEntries.addAll(searchResult.searchEntries)
                LDAPTestUtils.assertHasControl(searchResult, SimplePagedResultsControl.PAGED_RESULTS_OID)
                SimplePagedResultsControl.get(searchResult).let { responseControl ->
                    resumeCookie = if (responseControl.moreResultsToReturn()) {
                        responseControl.cookie
                    } else {
                        null
                    }
                }
            }
            searchResultEntries
        }
    }

    /**
     * Level 1 - Search functions locked to specific nodes, based on generic search function
     */
    private val searchInKafkaNode = searchWithPagination(env.ldapGroup.ldapGroupBase, SearchScope.ONE)
    private val searchInServiceAccountsNode = searchXInY(env.ldapGroup.ldapSrvUserBase, SearchScope.SUB)
    private val searchInUserAccountsNode = searchXInY(inheritDNTail(env.ldapGroup.ldapSrvUserBase, env.ldapAuthenticate.ldapAuthUserBase),
        SearchScope.SUB)

    /**
     * Level 2 - Search functions getting attributes, based on search functions locked to nodes
     */
    private val searchGetMembershipKN = searchInKafkaNode(env.ldapGroup.ldapGrpMemberAttrName)
    private val searchGetNamesKN = searchInKafkaNode(env.ldapGroup.ldapGroupAttrName)

    private val searchGetDNSAN = searchInServiceAccountsNode(SearchRequest.NO_ATTRIBUTES)
    private val searchGetDNUAN = searchInUserAccountsNode(SearchRequest.NO_ATTRIBUTES)

    /**
     * Level 3 - Useful base functions, based on search functions returning attributes
     */
    private fun getMembersInKafkaGroup(groupName: String, exists: Boolean = true): List<String> =
        if (!exists) emptyList()
        else
            searchGetMembershipKN(Filter.createEqualityFilter(env.ldapGroup.ldapGroupAttrName, groupName))
                .flatMap { it.getAttribute(env.ldapGroup.ldapGrpMemberAttrName)?.values?.toList() ?: emptyList() }

    private fun getKafkaGroupNames(): List<String> =
        searchGetNamesKN(Filter.createEqualityFilter("objectClass", "group"))
            .map { it.getAttribute(env.ldapGroup.ldapGroupAttrName).value }

    private fun getServiceUserDN(userName: String): String =
        searchGetDNSAN(Filter.createEqualityFilter(env.ldapCommon.ldapUserAttrName, userName))
            .let { searchRes ->
                when (searchRes.resultCode == ResultCode.SUCCESS && searchRes.entryCount == 1) {
                    true -> searchRes.searchEntries[0].dn
                    false -> ""
                }
            }

    private fun getUserDN(userName: String): String =
        searchGetDNUAN(Filter.createEqualityFilter(env.ldapCommon.ldapUserAttrName, userName))
            .let { searchRes ->
                when (searchRes.resultCode == ResultCode.SUCCESS && searchRes.entryCount == 1) {
                    true -> searchRes.searchEntries[0].dn
                    false -> ""
                }
            }

    /**
     * Level 4 - Useful base function, based on base functions
     */
    private fun groupEmpty(groupName: String) = getMembersInKafkaGroup(groupName).isEmpty()

    private fun resolveUserDN(userName: String) =
        if (isNAVIdent(userName)) getUserDN(userName) else getServiceUserDN(userName)

    private fun inheritDNTail(srcDN: String, trgDN: String): String {

        val uRDNS = DN(trgDN).rdNs
        val sRDNS = DN(srcDN).rdNs

        val uInd = uRDNS.indices
        val sInd = sRDNS.indices

        uRDNS[uInd.last - 1] = sRDNS[sInd.last - 1]
        uRDNS[uInd.last] = sRDNS[sInd.last]

        return uRDNS.joinToString(",")
    }

    companion object {

        val log = KotlinLogging.logger { }

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

        fun LDAPResult.simplify(): SLDAPResult = SLDAPResult(this.resultCode, this.diagnosticMessage ?: "")
    }
}

/**
 * Enum class KafkaGroupType with LDAP group prefix included
 * Each topic has 2 groups
 * - a producer group with members allowed to produce events to topic
 * - a consumer group with members allowed to consume events from topic
 */
enum class KafkaGroupType(val prefix: String) {
    @SerializedName("PRODUCER")
    PRODUCER("KP-"),
    @SerializedName("CONSUMER")
    CONSUMER("KC-"),
    @SerializedName("MANAGER")
    MANAGER("KM-")
}

fun KafkaGroupType.into(): List<AclOperation> = when (this) {
    KafkaGroupType.PRODUCER -> listOf(AclOperation.DESCRIBE, AclOperation.WRITE, AclOperation.CREATE)
    else -> listOf(AclOperation.DESCRIBE, AclOperation.READ)
}

fun KafkaGroupType.intoAcls(topicName: String): List<AclBinding> = into()
    .map { AccessControlEntry("Group:${toGroupName(prefix, topicName)}", "*", it, AclPermissionType.ALLOW) }
    .map { AclBinding(ResourcePattern(ResourceType.TOPIC, topicName, PatternType.LITERAL), it) }

/**
 * Enum class KafkaGroupOperation
 * ADD - add a new group member
 * REMOVE - remove a group member from group
 */
enum class GroupMemberOperation {
    @SerializedName("ADD")
    ADD,
    @SerializedName("REMOVE")
    REMOVE
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
 * A simpler version of LDAPResult, giving kafka API the resultCode and diagnostic message
 */
data class SLDAPResult(
    val resultCode: ResultCode = ResultCode.INSUFFICIENT_ACCESS_RIGHTS,
    val message: String = "Not authorized"
)

/**
 * data class KafkaGroup as result from functions iterating KafkaGroupType - see groupTypesCatch
 */
data class KafkaGroup(
    val type: KafkaGroupType = KafkaGroupType.MANAGER,
    val name: String = "",
    val members: List<String> = emptyList(),
    val ldapResult: SLDAPResult = LDAPResult(50, ResultCode.INSUFFICIENT_ACCESS_RIGHTS).simplify()
)

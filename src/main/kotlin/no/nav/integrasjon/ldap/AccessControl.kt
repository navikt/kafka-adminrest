package no.nav.integrasjon.ldap

enum class AccessCode { USER_NOT_FOUND, NAV_USER_NOT_ALLOWED, MANAGER_GROUP_NOT_ALLOWED, OK, TOO_MANY_GROUPS }

data class Access(
    val grant: Boolean,
    val accessCode: AccessCode,
    val result: SLDAPResult
)

class AccessControl(
    private val updateEntry: UpdateKafkaGroupMember,
    private val instance: LDAPGroup
) {

    fun resolveUserDN() =
        when {
            instance.isManagerGroup(updateEntry.member) -> instance.getGroupDN(updateEntry.member)
            instance.isNAVIdent(updateEntry.member) -> instance.getUserDN(updateEntry.member)
            else -> instance.getServiceUserDN(updateEntry.member)
        }

    fun validate(userDn: String, topicName: String) =
        when {
            userDn.isEmpty() ->
                Access(
                    false, AccessCode.USER_NOT_FOUND, SLDAPResult()
                )
            updateEntry.role != KafkaGroupType.MANAGER && instance.isNAVIdent(updateEntry.member) ->
                Access(
                    false, AccessCode.NAV_USER_NOT_ALLOWED, SLDAPResult()
                )
            updateEntry.role != KafkaGroupType.MANAGER && instance.isManagerGroup(updateEntry.member) ->
                Access(
                    false, AccessCode.MANAGER_GROUP_NOT_ALLOWED, SLDAPResult()
                )
            updateEntry.role == KafkaGroupType.MANAGER && instance.isManagerGroup(updateEntry.member) && !instance.findMembersAsGroup(
                toGroupName(KafkaGroupType.MANAGER.prefix, topicName)
            ).isEmpty() ->
                Access(
                    false, AccessCode.TOO_MANY_GROUPS, SLDAPResult()
                )
            else -> Access(
                true, AccessCode.OK, SLDAPResult()
            )
        }
}
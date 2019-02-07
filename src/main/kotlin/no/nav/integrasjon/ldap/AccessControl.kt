package no.nav.integrasjon.ldap

enum class AccessCode { USER_NOT_FOUND, NAV_USER_NOT_ALLOWED, MANAGER_GROUP_NOT_ALLOWED, OK, TOO_MANY_GROUPS }

class AccessControl(
    private val userDn: String,
    private val topicName: String,
    private val updateEntry: UpdateKafkaGroupMember,
    private val instance: LDAPGroup
) {

    fun validateAccess() =
        when {
            userDn.isEmpty() ->
                Triple(
                    false, AccessCode.USER_NOT_FOUND, SLDAPResult()
                )
            updateEntry.role != KafkaGroupType.MANAGER && instance.isNAVIdent(updateEntry.member) ->
                Triple(
                    false, AccessCode.NAV_USER_NOT_ALLOWED, SLDAPResult()
                )
            updateEntry.role != KafkaGroupType.MANAGER && instance.isManagerGroup(updateEntry.member) ->
                Triple(
                    false, AccessCode.MANAGER_GROUP_NOT_ALLOWED, SLDAPResult()
                )
            updateEntry.role == KafkaGroupType.MANAGER && instance.isManagerGroup(updateEntry.member) && !instance.findMembersAsGroup(
                toGroupName(KafkaGroupType.MANAGER.prefix, topicName)
            ).isEmpty() ->
                Triple(
                    false, AccessCode.TOO_MANY_GROUPS, SLDAPResult()
                )
            else -> Triple(
                true, AccessCode.OK, SLDAPResult()
            )
        }
}
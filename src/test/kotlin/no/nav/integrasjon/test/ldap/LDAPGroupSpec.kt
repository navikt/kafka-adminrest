package no.nav.integrasjon.test.ldap

import com.unboundid.ldap.sdk.ResultCode
import no.nav.integrasjon.FasitProperties
import no.nav.integrasjon.ldap.GroupMemberOperation
import no.nav.integrasjon.ldap.KafkaGroupType
import no.nav.integrasjon.ldap.LDAPGroup
import no.nav.integrasjon.ldap.UpdateKafkaGroupMember
import no.nav.integrasjon.test.common.InMemoryLDAPServer
import org.amshove.kluent.shouldBe
import org.amshove.kluent.shouldContainAll
import org.amshove.kluent.shouldEqual
import org.amshove.kluent.shouldEqualTo
import org.amshove.kluent.shouldHaveTheSameClassAs
import org.amshove.kluent.shouldNotContainAny
import org.spekframework.spek2.Spek
import org.spekframework.spek2.style.specification.describe

object LDAPGroupSpec : Spek({

    val fp = FasitProperties(
            "", "", "", "", "", "", "",
            ldapConnTimeout = 250,
            ldapUserAttrName = "uid",
            ldapAuthHost = "localhost",
            ldapAuthPort = InMemoryLDAPServer.LPORT,
            ldapAuthUserBase = "OU=Users,OU=NAV,OU=BusinessUnits,DC=test,DC=local",
            ldapHost = "localhost",
            ldapPort = InMemoryLDAPServer.LPORT,
            ldapSrvUserBase = "OU=ServiceAccounts,DC=test,DC=local",
            ldapGroupBase = "OU=kafka,OU=AccountGroupNotInRemedy,OU=Groups,OU=NAV,OU=BusinessUnits,DC=test,DC=local",
            ldapGroupAttrName = "cn",
            ldapGrpMemberAttrName = "member",
            ldapUser = "igroup",
            ldapPassword = "itest",
            ldapGroupInGroupBase = "OU=Groups,OU=NAV,OU=BusinessUnits,DC=test,DC=local"
    )

    describe("LDAPGroup class test specification") {

        beforeGroup { InMemoryLDAPServer.start() }

        context("Get all objectclass 'group' under ou=kafka - getKafkaGroups") {

            val existingGroups = listOf("KC-tpc-01", "KC-tpc-02", "KC-tpc-03", "KP-tpc-01", "KP-tpc-02", "KP-tpc-03")

            it("should return $existingGroups") {
                LDAPGroup(fp).use { lc -> lc.getKafkaGroups() } shouldContainAll existingGroups
            }
        }

        context("For a given topic, get prod. and cons. groups with members - getKafkaGroupsAndMembers") {

            val topics = mapOf(
                    "tpc-01" to listOf("uid=n000002,ou=Users,ou=NAV,ou=BusinessUnits,dc=test,dc=local"),
                    "tpc-02" to listOf(
                            "uid=srvc02,ou=ApplAccounts,ou=ServiceAccounts,dc=test,dc=local",
                            "uid=srvp01,ou=ServiceAccounts,dc=test,dc=local"
                    ),
                    "tpc-03" to listOf(
                            "uid=srvc01,ou=ServiceAccounts,dc=test,dc=local",
                            "uid=srvp02,ou=ApplAccounts,ou=ServiceAccounts,dc=test,dc=local",
                            "uid=n145821,ou=Users,ou=NAV,ou=BusinessUnits,dc=test,dc=local"
                    )
            )

            topics.forEach { topic, allMembers ->
                it("should return correct info for topic $topic") {
                    val kGroups = LDAPGroup(fp).use { lc -> lc.getKafkaGroupsAndMembers(topic) }

                    kGroups.size shouldEqualTo KafkaGroupType.values().size
                    kGroups.map { it.type } shouldContainAll KafkaGroupType.values()
                    kGroups.flatMap { it.members } shouldContainAll allMembers
                }
            }
        }

        context("For a given group name, get a list of members") {

            val groups = mapOf(
                    "KP-tpc-01" to emptyList(),
                    "KC-tpc-02" to listOf("uid=srvc02,ou=ApplAccounts,ou=ServiceAccounts,dc=test,dc=local"),
                    "KP-tpc-03" to listOf("uid=srvp02,ou=ApplAccounts,ou=ServiceAccounts,dc=test,dc=local")
            )

            groups.forEach { group, members ->
                it("should return $members for group $group") {
                    LDAPGroup(fp).use { lc -> lc.getKafkaGroupMembers(group) } shouldEqual members
                }
            }
        }

        context("Create kafka groups for topic tpc-04") {

            "tpc-04".let { topic ->
                it("should return 2 new groups when asking for all kafka groups") {
                    LDAPGroup(fp).use { lc ->
                        lc.createKafkaGroups(topic, "n145821")
                        lc.getKafkaGroups()
                    } shouldContainAll listOf("KP-$topic", "KC-$topic", "KM-$topic")
                }

                it("should report error when trying to create groups that exists") {
                    LDAPGroup(fp).use { lc ->
                        lc.createKafkaGroups(topic, "n145821")
                    }.map { it.ldapResult.resultCode != ResultCode.SUCCESS } shouldContainAll listOf(true, true)
                }
            }
        }

        context("Add service users as producer/consumer for topic tpc-04") {

            "tpc-04".let { topic ->
                it("should return the added srvp01 producer when getting group members") {
                    LDAPGroup(fp).use { lc ->
                        lc.updateKafkaGroupMembership(
                                topic,
                                UpdateKafkaGroupMember(
                                        KafkaGroupType.PRODUCER,
                                        GroupMemberOperation.ADD,
                                        "srvp01"
                                ))
                        lc.getKafkaGroupMembers("KP-$topic")
                    } shouldContainAll listOf(
                            "uid=srvp01,ou=ServiceAccounts,dc=test,dc=local")
                }

                it("should return the added Group_00020ec3-6592-4415-a563-1ed6768d6086 MANAGER when getting group members") {
                    LDAPGroup(fp).use { lc ->
                        lc.updateKafkaGroupMembership(
                            topic,
                            UpdateKafkaGroupMember(
                                KafkaGroupType.MANAGER,
                                GroupMemberOperation.ADD,
                                "Group_00020ec3-6592-4415-a563-1ed6768d6086"
                            ))
                        lc.getKafkaGroupMembers("KM-$topic")
                    } shouldContainAll listOf(
                        "uid=n145821,ou=Users,ou=NAV,ou=BusinessUnits,dc=test,dc=local",
                        "cn=Group_00020ec3-6592-4415-a563-1ed6768d6086,OU=O365Groups,OU=Groups,OU=NAV,OU=BusinessUnits,DC=test,DC=local")
                }

                it("should throw an Error adding 0000-GA-BASTA_SUPERUSER as MANAGER") {
                    try {
                        LDAPGroup(fp).use { lc ->
                            lc.updateKafkaGroupMembership(
                                topic,
                                UpdateKafkaGroupMember(
                                    KafkaGroupType.MANAGER,
                                    GroupMemberOperation.ADD,
                                    "0000-GA-BASTA_SUPERUSER"
                                ))
                            lc.getKafkaGroupMembers("KM-$topic")
                        }
                        } catch (e: LDAPGroup.GroupInGroupException) {
                            e.shouldHaveTheSameClassAs(LDAPGroup.GroupInGroupException("Cannot have to groups: 0000-GA-BASTA_SUPERUSER as Manager"))
                        }
                }

                it("should throw error adding 0000-GA-BASTA_SUPERUSER as Producer") {
                    try {
                        LDAPGroup(fp).use { lc ->
                            lc.updateKafkaGroupMembership(
                                topic,
                                UpdateKafkaGroupMember(
                                    KafkaGroupType.PRODUCER,
                                    GroupMemberOperation.ADD,
                                    "0000-GA-BASTA_SUPERUSER"
                                ))
                        }
                    } catch (e: LDAPGroup.UserNotAllowedException) {
                        e.shouldHaveTheSameClassAs(LDAPGroup.UserNotAllowedException("Cannot have 0000-GA-BASTA_SUPERUSER as consumer/producer"))
                    }
                }

                it("should return the added srvp02 producer when getting group members") {
                    LDAPGroup(fp).use { lc ->
                        lc.updateKafkaGroupMembership(
                                topic,
                                UpdateKafkaGroupMember(
                                        KafkaGroupType.PRODUCER,
                                        GroupMemberOperation.ADD,
                                        "srvp02"
                                ))
                        lc.getKafkaGroupMembers("KP-$topic")
                    } shouldContainAll listOf(
                            "uid=srvp01,ou=ServiceAccounts,dc=test,dc=local",
                            "uid=srvp02,ou=ApplAccounts,ou=ServiceAccounts,dc=test,dc=local")
                }

                it("should return the added srvc01 producer when getting group members") {
                    LDAPGroup(fp).use { lc ->
                        lc.updateKafkaGroupMembership(
                                topic,
                                UpdateKafkaGroupMember(
                                        KafkaGroupType.CONSUMER,
                                        GroupMemberOperation.ADD,
                                        "srvc01"
                                ))
                        lc.getKafkaGroupMembers("KC-$topic")
                    } shouldContainAll listOf(
                            "uid=srvc01,ou=ServiceAccounts,dc=test,dc=local")
                }

                it("should return the added srvc02 producer when getting group members") {
                    LDAPGroup(fp).use { lc ->
                        lc.updateKafkaGroupMembership(
                                topic,
                                UpdateKafkaGroupMember(
                                        KafkaGroupType.CONSUMER,
                                        GroupMemberOperation.ADD,
                                        "srvc02"
                                ))
                        lc.getKafkaGroupMembers("KC-$topic")
                    } shouldContainAll listOf(
                            "uid=srvc01,ou=ServiceAccounts,dc=test,dc=local",
                            "uid=srvc02,ou=ApplAccounts,ou=ServiceAccounts,dc=test,dc=local")
                }

                it("should give ok when trying to add existing member") {

                    LDAPGroup(fp).use { lc ->
                        lc.updateKafkaGroupMembership(
                                topic,
                                UpdateKafkaGroupMember(
                                        KafkaGroupType.CONSUMER,
                                        GroupMemberOperation.ADD,
                                        "srvc02"
                                ))
                    }.resultCode shouldBe ResultCode.SUCCESS
                }
            }
        }

        context("Remove service users as producer/consumer for topic tpc-04") {

            "tpc-04".let { topic ->
                it("should return group members without removed srvp01") {
                    LDAPGroup(fp).use { lc ->
                        lc.updateKafkaGroupMembership(
                                topic,
                                UpdateKafkaGroupMember(
                                        KafkaGroupType.PRODUCER,
                                        GroupMemberOperation.REMOVE,
                                        "srvp01"
                                ))
                        lc.getKafkaGroupMembers("KP-$topic")
                    } shouldNotContainAny listOf(
                            "uid=srvp01,ou=ServiceAccounts,dc=test,dc=local")
                }

                it("should return group members without removed srvc01") {
                    LDAPGroup(fp).use { lc ->
                        lc.updateKafkaGroupMembership(
                                topic,
                                UpdateKafkaGroupMember(
                                        KafkaGroupType.CONSUMER,
                                        GroupMemberOperation.REMOVE,
                                        "srvc01"
                                ))
                        lc.getKafkaGroupMembers("KC-$topic")
                    } shouldNotContainAny listOf(
                            "uid=srvc01,ou=ServiceAccounts,dc=test,dc=local")
                }

                it("should give ok when trying to remove non-existing member") {

                    LDAPGroup(fp).use { lc ->
                        lc.updateKafkaGroupMembership(
                                topic,
                                UpdateKafkaGroupMember(
                                        KafkaGroupType.CONSUMER,
                                        GroupMemberOperation.REMOVE,
                                        "srvc01"
                                ))
                    }.resultCode shouldBe ResultCode.SUCCESS
                }
            }
        }

        context("Delete kafka groups for topic tpc-04") {

            "tpc-04".let { topic ->
                it("should not return those 3 groups when asking for all kafka groups") {
                    LDAPGroup(fp).use { lc ->
                        lc.deleteKafkaGroups(topic)
                        lc.getKafkaGroups()
                    } shouldNotContainAny listOf("KP-$topic", "KC-$topic", "KM-$topic")
                }

                it("should report error when trying to delete non-existing groups") {
                    LDAPGroup(fp).use { lc ->
                        lc.deleteKafkaGroups(topic)
                    }.map { it.ldapResult.resultCode != ResultCode.SUCCESS } shouldContainAll listOf(true, true)
                }
            }
        }

        context("Get all groups and members of groups in a group") {
            "KM-tpc-01".let { topic ->
                it("should return all members groups in a Kafka groups") {
                    LDAPGroup(fp).use { lc ->
                        lc.findMembersAsGroup(topic)
                            .map { group ->
                                lc.getGroupInGroupMembers(group) }.get(index = 0) shouldContainAll listOf("uid=n000002,ou=Users,ou=NAV,ou=BusinessUnits,dc=test,dc=local", "uid=n000003,ou=Users,ou=NAV,ou=BusinessUnits,dc=test,dc=local")
                    }
                }
            }
        }

        context("Verify management access for topics") {
            val topics = mapOf(
                    Pair("tpc-01", "n000002") to true,
                    Pair("tpc-02", "n141414") to false,
                    Pair("tpc-03", "n145821") to true,
                    Pair("tpc-01", "n000003") to true
            )

            topics.forEach { pair, result ->
                it("should return isManager is $result for topic $pair") {
                    val isMng = LDAPGroup(fp).use { lc -> lc.userIsManager(pair.first, pair.second) }

                    isMng shouldEqualTo result
                }
            }
        }

        afterGroup { InMemoryLDAPServer.stop() }
    }
})

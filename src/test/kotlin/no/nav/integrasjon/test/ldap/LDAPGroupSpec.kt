package no.nav.integrasjon.test.ldap

import com.unboundid.ldap.sdk.LDAPException
import com.unboundid.ldap.sdk.ResultCode
import no.nav.integrasjon.FasitProperties
import no.nav.integrasjon.ldap.LDAPGroup
import no.nav.integrasjon.test.common.InMemoryLDAPServer
import org.amshove.kluent.*
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.*

object LDAPGroupSpec : Spek({

    val fp = FasitProperties(
            "","","","","","","",
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
            ldapPassword = "itest"
    )

    describe("LDAPGroup class test specification") {

        beforeGroup { InMemoryLDAPServer.start() }

        context("Get all objectclass 'group' under ou=kafka - getKafkaGroups") {

            val existingGroups = listOf("KC-tpc-01","KC-tpc-02","KC-tpc-03","KP-tpc-01","KP-tpc-02","KP-tpc-03")

            it("should return $existingGroups") {
                LDAPGroup(fp).use { lc -> lc.getKafkaGroups() } shouldContainAll existingGroups
            }
        }

        context("For a given topic, get prod. and cons. groups with members - getKafkaGroupsAndMembers") {

            "tpc-01".let {
                it("should return correct info for $it") {
                    val kGroups = LDAPGroup(fp).use { lc -> lc.getKafkaGroupsAndMembers(it) }

                    kGroups.size shouldEqualTo 2
                    kGroups.map { it.groupType } shouldContainAll LDAPGroup.Companion.KafkaGroupType.values()
                    kGroups.flatMap { it.members } shouldEqual emptyList()
                }
            }

            "tpc-02".let {
                it("should return correct info for $it") {
                    val kGroups = LDAPGroup(fp).use { lc -> lc.getKafkaGroupsAndMembers(it) }

                    kGroups.size shouldEqualTo 2
                    kGroups.map { it.groupType } shouldContainAll LDAPGroup.Companion.KafkaGroupType.values()
                    kGroups.flatMap { it.members } shouldContainAll  listOf(
                            "uid=srvc02,ou=ApplAccounts,ou=ServiceAccounts,dc=test,dc=local",
                            "uid=srvp01,ou=ServiceAccounts,dc=test,dc=local"
                    )
                }
            }

            "tpc-03".let {
                it("should return correct info for $it") {
                    val kGroups = LDAPGroup(fp).use { lc -> lc.getKafkaGroupsAndMembers(it) }

                    kGroups.size shouldEqualTo 2
                    kGroups.map { it.groupType } shouldContainAll LDAPGroup.Companion.KafkaGroupType.values()
                    kGroups.flatMap { it.members } shouldContainAll  listOf(
                            "uid=srvc01,ou=ServiceAccounts,dc=test,dc=local",
                            "uid=srvp02,ou=ApplAccounts,ou=ServiceAccounts,dc=test,dc=local"
                    )
                }
            }
        }

        context("For a given group name, get a list of members") {

            "KP-tpc-01".let {
                it("should return correct members for $it") {
                    LDAPGroup(fp).use { lc -> lc.getKafkaGroupMembers(it) } shouldEqual emptyList()
                }
            }

            "KC-tpc-02".let {
                it("should return correct members for $it") {
                    LDAPGroup(fp).use { lc -> lc.getKafkaGroupMembers(it) } shouldContainAll listOf(
                            "uid=srvc02,ou=ApplAccounts,ou=ServiceAccounts,dc=test,dc=local")
                }
            }

            "KP-tpc-03".let {
                it("should return correct members for $it") {
                    LDAPGroup(fp).use { lc -> lc.getKafkaGroupMembers(it) } shouldContainAll listOf(
                            "uid=srvp02,ou=ApplAccounts,ou=ServiceAccounts,dc=test,dc=local")
                }
            }
        }

        context("Create kafka groups for a topic") {

            "tpc-04".let { topic ->
                it("should return 2 new groups when asking for all kafka groups") {
                    LDAPGroup(fp).use { lc ->
                        lc.createKafkaGroups(topic)
                        lc.getKafkaGroups()
                    } shouldContainAll listOf("KP-$topic","KC-$topic")
                }

                it("should report error when trying to create groups that exists") {
                    LDAPGroup(fp).use { lc ->
                        lc.createKafkaGroups(topic)
                    }.map { it.result.resultCode != ResultCode.SUCCESS } shouldContainAll listOf(true,true)
                }

            }
        }

        context("Delete kafka groups for a topic") {

            "tpc-04".let { topic ->
                it("should not return those 2 groups when asking for all kafka groups") {
                    LDAPGroup(fp).use { lc ->
                        lc.deleteKafkaGroups(topic)
                        lc.getKafkaGroups()
                    } shouldNotContainAny listOf("KP-$topic","KC-$topic")
                }

                it("should report error when trying to delete non-existing groups") {
                    LDAPGroup(fp).use { lc ->
                        lc.deleteKafkaGroups(topic)
                    }.map { it.result.resultCode != ResultCode.SUCCESS } shouldContainAll listOf(true,true)
                }
            }
        }

        context("Add service users as producer/consumer for a topic") {

            "tpc-01".let { topic ->
                it("should return the added srvp01 producer when getting group members") {
                    LDAPGroup(fp).use { lc ->
                        lc.updateKafkaGroupMembership(
                                topic,
                                LDAPGroup.Companion.UpdateKafkaGroupMember(
                                        LDAPGroup.Companion.KafkaGroupType.PRODUCER,
                                        LDAPGroup.Companion.GroupMemberOperation.ADD,
                                        "srvp01"
                                ))
                        lc.getKafkaGroupMembers("KP-$topic")
                    } shouldContainAll listOf(
                            "uid=srvp01,ou=ServiceAccounts,dc=test,dc=local")
                }

                it("should return the added srvp02 producer when getting group members") {
                    LDAPGroup(fp).use { lc ->
                        lc.updateKafkaGroupMembership(
                                topic,
                                LDAPGroup.Companion.UpdateKafkaGroupMember(
                                        LDAPGroup.Companion.KafkaGroupType.PRODUCER,
                                        LDAPGroup.Companion.GroupMemberOperation.ADD,
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
                                LDAPGroup.Companion.UpdateKafkaGroupMember(
                                        LDAPGroup.Companion.KafkaGroupType.CONSUMER,
                                        LDAPGroup.Companion.GroupMemberOperation.ADD,
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
                                LDAPGroup.Companion.UpdateKafkaGroupMember(
                                        LDAPGroup.Companion.KafkaGroupType.CONSUMER,
                                        LDAPGroup.Companion.GroupMemberOperation.ADD,
                                        "srvc02"
                                ))
                        lc.getKafkaGroupMembers("KC-$topic")
                    } shouldContainAll listOf(
                            "uid=srvc01,ou=ServiceAccounts,dc=test,dc=local",
                            "uid=srvc02,ou=ApplAccounts,ou=ServiceAccounts,dc=test,dc=local")
                }

                it("should give exception when trying to add existing member") {
                    val res = try{
                        LDAPGroup(fp).use { lc ->
                            lc.updateKafkaGroupMembership(
                                    topic,
                                    LDAPGroup.Companion.UpdateKafkaGroupMember(
                                            LDAPGroup.Companion.KafkaGroupType.CONSUMER,
                                            LDAPGroup.Companion.GroupMemberOperation.ADD,
                                            "srvc02"
                                    ))
                        }
                    }
                    catch (e: LDAPException) {e.toLDAPResult()}

                    res.resultCode shouldNotBe ResultCode.SUCCESS
                }
            }
        }

        context("Remove service users as producer/consumer for a topic") {

            "tpc-01".let { topic ->
                it("should return group members without removed srvp01") {
                    LDAPGroup(fp).use { lc ->
                        lc.updateKafkaGroupMembership(
                                topic,
                                LDAPGroup.Companion.UpdateKafkaGroupMember(
                                        LDAPGroup.Companion.KafkaGroupType.PRODUCER,
                                        LDAPGroup.Companion.GroupMemberOperation.REMOVE,
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
                                LDAPGroup.Companion.UpdateKafkaGroupMember(
                                        LDAPGroup.Companion.KafkaGroupType.CONSUMER,
                                        LDAPGroup.Companion.GroupMemberOperation.REMOVE,
                                        "srvc01"
                                ))
                        lc.getKafkaGroupMembers("KC-$topic")
                    } shouldNotContainAny listOf(
                            "uid=srvc01,ou=ServiceAccounts,dc=test,dc=local")
                }

                it("should give exception when trying to remove non-existing member") {
                    val res = try{
                        LDAPGroup(fp).use { lc ->
                            lc.updateKafkaGroupMembership(
                                    topic,
                                    LDAPGroup.Companion.UpdateKafkaGroupMember(
                                            LDAPGroup.Companion.KafkaGroupType.CONSUMER,
                                            LDAPGroup.Companion.GroupMemberOperation.REMOVE,
                                            "srvc01"
                                    ))
                        }
                    }
                    catch (e: LDAPException) {e.toLDAPResult()}

                    res.resultCode shouldNotBe ResultCode.SUCCESS
                }
            }
        }

        afterGroup { InMemoryLDAPServer.stop() }
    }
})
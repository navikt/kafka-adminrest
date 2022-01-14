package no.nav.integrasjon.test.ldap

import com.unboundid.ldap.sdk.ResultCode
import no.nav.integrasjon.Environment
import no.nav.integrasjon.ldap.GroupMemberOperation
import no.nav.integrasjon.ldap.KafkaGroupType
import no.nav.integrasjon.ldap.LDAPGroup
import no.nav.integrasjon.ldap.UpdateKafkaGroupMember
import no.nav.integrasjon.test.common.InMemoryLDAPServer
import org.amshove.kluent.shouldBe
import org.amshove.kluent.shouldBeEqualTo
import org.amshove.kluent.shouldContainAll
import org.amshove.kluent.shouldNotContainAny
import org.spekframework.spek2.Spek
import org.spekframework.spek2.style.specification.describe

object LDAPGroupSpec : Spek({

    val environment = Environment(
        ldapAuthenticate = Environment.LdapAuthenticate(ldapAuthPort = InMemoryLDAPServer.LPORT),
        ldapGroup = Environment.LdapGroup(ldapPort = InMemoryLDAPServer.LPORT),
        superusers = listOf("admin", "other-admin")
    )

    describe("LDAPGroup class test specification") {

        beforeGroup { InMemoryLDAPServer.start() }

        context("Get all objectclass 'group' under ou=kafka - getKafkaGroups") {

            val existingGroups = listOf("KC-tpc-01", "KC-tpc-02", "KC-tpc-03", "KP-tpc-01", "KP-tpc-02", "KP-tpc-03")

            it("should return $existingGroups") {
                LDAPGroup(environment).use { lc -> lc.getKafkaGroups() } shouldContainAll existingGroups
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

            topics.forEach { (topic, allMembers) ->
                it("should return correct info for topic $topic") {
                    val kGroups = LDAPGroup(environment).use { lc -> lc.getKafkaGroupsAndMembers(topic) }

                    kGroups.size shouldBeEqualTo KafkaGroupType.values().size
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

            groups.forEach { (group, members) ->
                it("should return $members for group $group") {
                    LDAPGroup(environment).use { lc -> lc.getKafkaGroupMembers(group) } shouldBeEqualTo members
                }
            }
        }

        context("Create kafka groups for topic tpc-04") {

            "tpc-04".let { topic ->
                it("should return 2 new groups when asking for all kafka groups") {
                    LDAPGroup(environment).use { lc ->
                        lc.createKafkaGroups(topic, "n145821")
                        lc.getKafkaGroups()
                    } shouldContainAll listOf("KP-$topic", "KC-$topic", "KM-$topic")
                }

                it("should report error when trying to create groups that exists") {
                    LDAPGroup(environment).use { lc ->
                        lc.createKafkaGroups(topic, "n145821")
                    }.map { it.ldapResult.resultCode != ResultCode.SUCCESS } shouldContainAll listOf(true, true)
                }
            }
        }

        context("Add service users as producer/consumer for topic tpc-04") {

            "tpc-04".let { topic ->
                it("should return the added srvp01 producer when getting group members") {
                    LDAPGroup(environment).use { lc ->
                        lc.updateKafkaGroupMembership(
                            topic,
                            UpdateKafkaGroupMember(
                                KafkaGroupType.PRODUCER,
                                GroupMemberOperation.ADD,
                                "srvp01"
                            )
                        )
                        lc.getKafkaGroupMembers("KP-$topic")
                    } shouldContainAll listOf(
                        "uid=srvp01,ou=ServiceAccounts,dc=test,dc=local"
                    )
                }

                it("should return the added srvp02 producer when getting group members") {
                    LDAPGroup(environment).use { lc ->
                        lc.updateKafkaGroupMembership(
                            topic,
                            UpdateKafkaGroupMember(
                                KafkaGroupType.PRODUCER,
                                GroupMemberOperation.ADD,
                                "srvp02"
                            )
                        )
                        lc.getKafkaGroupMembers("KP-$topic")
                    } shouldContainAll listOf(
                        "uid=srvp01,ou=ServiceAccounts,dc=test,dc=local",
                        "uid=srvp02,ou=ApplAccounts,ou=ServiceAccounts,dc=test,dc=local"
                    )
                }

                it("should return the added srvc01 producer when getting group members") {
                    LDAPGroup(environment).use { lc ->
                        lc.updateKafkaGroupMembership(
                            topic,
                            UpdateKafkaGroupMember(
                                KafkaGroupType.CONSUMER,
                                GroupMemberOperation.ADD,
                                "srvc01"
                            )
                        )
                        lc.getKafkaGroupMembers("KC-$topic")
                    } shouldContainAll listOf(
                        "uid=srvc01,ou=ServiceAccounts,dc=test,dc=local"
                    )
                }

                it("should return the added srvc02 producer when getting group members") {
                    LDAPGroup(environment).use { lc ->
                        lc.updateKafkaGroupMembership(
                            topic,
                            UpdateKafkaGroupMember(
                                KafkaGroupType.CONSUMER,
                                GroupMemberOperation.ADD,
                                "srvc02"
                            )
                        )
                        lc.getKafkaGroupMembers("KC-$topic")
                    } shouldContainAll listOf(
                        "uid=srvc01,ou=ServiceAccounts,dc=test,dc=local",
                        "uid=srvc02,ou=ApplAccounts,ou=ServiceAccounts,dc=test,dc=local"
                    )
                }

                it("should give ok when trying to add existing member") {

                    LDAPGroup(environment).use { lc ->
                        lc.updateKafkaGroupMembership(
                            topic,
                            UpdateKafkaGroupMember(
                                KafkaGroupType.CONSUMER,
                                GroupMemberOperation.ADD,
                                "srvc02"
                            )
                        )
                    }.resultCode shouldBe ResultCode.SUCCESS
                }
            }
        }

        context("Remove service users as producer/consumer for topic tpc-04") {

            "tpc-04".let { topic ->
                it("should return group members without removed srvp01") {
                    LDAPGroup(environment).use { lc ->
                        lc.updateKafkaGroupMembership(
                            topic,
                            UpdateKafkaGroupMember(
                                KafkaGroupType.PRODUCER,
                                GroupMemberOperation.REMOVE,
                                "srvp01"
                            )
                        )
                        lc.getKafkaGroupMembers("KP-$topic")
                    } shouldNotContainAny listOf(
                        "uid=srvp01,ou=ServiceAccounts,dc=test,dc=local"
                    )
                }

                it("should return group members without removed srvc01") {
                    LDAPGroup(environment).use { lc ->
                        lc.updateKafkaGroupMembership(
                            topic,
                            UpdateKafkaGroupMember(
                                KafkaGroupType.CONSUMER,
                                GroupMemberOperation.REMOVE,
                                "srvc01"
                            )
                        )
                        lc.getKafkaGroupMembers("KC-$topic")
                    } shouldNotContainAny listOf(
                        "uid=srvc01,ou=ServiceAccounts,dc=test,dc=local"
                    )
                }

                it("should give ok when trying to remove non-existing member") {

                    LDAPGroup(environment).use { lc ->
                        lc.updateKafkaGroupMembership(
                            topic,
                            UpdateKafkaGroupMember(
                                KafkaGroupType.CONSUMER,
                                GroupMemberOperation.REMOVE,
                                "srvc01"
                            )
                        )
                    }.resultCode shouldBe ResultCode.SUCCESS
                }
            }
        }

        context("Delete kafka groups for topic tpc-04") {

            "tpc-04".let { topic ->
                it("should not return those 3 groups when asking for all kafka groups") {
                    LDAPGroup(environment).use { lc ->
                        lc.deleteKafkaGroups(topic)
                        lc.getKafkaGroups()
                    } shouldNotContainAny listOf("KP-$topic", "KC-$topic", "KM-$topic")
                }

                it("should report error when trying to delete non-existing groups") {
                    LDAPGroup(environment).use { lc ->
                        lc.deleteKafkaGroups(topic)
                    }.map { it.ldapResult.resultCode != ResultCode.SUCCESS } shouldContainAll listOf(true, true)
                }
            }
        }

        context("Verify management access for topics") {
            val topics = mapOf(
                Pair("tpc-01", "n000002") to true,
                Pair("tpc-02", "n141414") to false,
                Pair("tpc-03", "n145821") to true,
                Pair("tpc-01", "admin") to true,
                Pair("tpc-01", "other-admin") to true,
                Pair("tpc-01", "not-admin") to false,
            )

            topics.forEach { (pair, result) ->
                it("should return isManager is $result for topic $pair") {
                    val isMng = LDAPGroup(environment).use { lc -> lc.userIsManager(pair.first, pair.second) }

                    isMng shouldBeEqualTo result
                }
            }
        }

        afterGroup { InMemoryLDAPServer.stop() }
    }
})

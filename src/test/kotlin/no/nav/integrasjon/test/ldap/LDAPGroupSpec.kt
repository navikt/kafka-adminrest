package no.nav.integrasjon.test.ldap

import no.nav.integrasjon.FasitProperties
import no.nav.integrasjon.ldap.LDAPGroup
import no.nav.integrasjon.test.common.InMemoryLDAPServer
import org.amshove.kluent.shouldContainAll
import org.amshove.kluent.shouldEqual
import org.amshove.kluent.shouldEqualTo
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.context
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it

object LDAPGroupSpec : Spek({

    val fp = FasitProperties(
            "","","","","","","",
            250,
            "uid",
            "localhost",
            11636,
            "OU=Users,OU=NAV,OU=BusinessUnits,DC=test,DC=local",
            "localhost",
            11636,
            "OU=ServiceAccounts,DC=test,DC=local",
            "OU=kafka,OU=AccountGroupNotInRemedy,OU=Groups,OU=NAV,OU=BusinessUnits,DC=test,DC=local",
            "cn",
            "member",
            "itest",
            "itest")

    describe("LDAPGroup class test specification") {

        beforeGroup { InMemoryLDAPServer.start() }

        context("Get all objectclass 'group' under ou=kafka - getKafkaGroups") {

            val existingGroups = listOf("KC-tpc-01","KC-tpc-02","KC-tpc-03","KP-tpc-01","KP-tpc-02","KP-tpc-03")

            it("should return $existingGroups") {
                LDAPGroup(fp).use { lc -> lc.getKafkaGroups() } shouldEqual existingGroups
            }
        }

        context("For a given topic, get prod. and cons. groups with members - getKafkaGroupsAndMembers") {

            val t1 = "tpc-01"

            it("should return correct info for $t1") {
                val kGroups = LDAPGroup(fp).use { lc -> lc.getKafkaGroupsAndMembers(t1) }

                kGroups.size shouldEqualTo 2
                kGroups.map { it.groupType } shouldContainAll LDAPGroup.Companion.KafkaGroupType.values()
                kGroups.flatMap { it.members } shouldEqual emptyList()
            }
        }

        afterGroup { InMemoryLDAPServer.stop() }
    }
})
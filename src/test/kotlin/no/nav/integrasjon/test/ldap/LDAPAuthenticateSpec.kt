package no.nav.integrasjon.test.ldap

import no.nav.integrasjon.FasitProperties
import no.nav.integrasjon.ldap.LDAPAuthenticate
import no.nav.integrasjon.test.common.InMemoryLDAPServer
import org.amshove.kluent.shouldEqualTo
import org.spekframework.spek2.Spek
import org.spekframework.spek2.style.specification.describe

object LDAPAuthenticateSpec : Spek({

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
        ldapPassword = "itest"
    )

    describe("LDAPauthenticate class test specification") {

        beforeGroup { InMemoryLDAPServer.start() }

        context("authenticate should work correctly for NAV ident and srv user") {

            val users = mapOf(
                Pair("srvp01", "dummy") to true,
                Pair("n000001", "itest1") to true,
                Pair("notExisting", "wildGuess") to false,
                Pair("n000002", "wrongPassword") to false,
                Pair("srvc02", "dummy") to true,
                Pair("n145821", "itest3") to true
            )

            users.forEach { (user, result) ->
                it("should return $result for user ${user.first}") {
                    LDAPAuthenticate(fp).use { lc ->
                        lc.canUserAuthenticate(user.first, user.second)
                    } shouldEqualTo result
                }
            }
        }

        afterGroup { InMemoryLDAPServer.stop() }
    }
})

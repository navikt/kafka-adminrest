package no.nav.integrasjon.test.ldap

import no.nav.integrasjon.FasitProperties
import no.nav.integrasjon.ldap.LDAPAuthenticate
import no.nav.integrasjon.test.common.InMemoryLDAPServer
import org.amshove.kluent.shouldEqualTo
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.context
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it

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
                    Pair("srvp01", "dummy") to false,
                    Pair("iauth", "itest") to true,
                    Pair("notExisting", "wildGuess") to false,
                    Pair("iauth2", "wrongPassword") to false,
                    Pair("srvc02", "dummy") to false
            )

            users.forEach { user, result ->
                it("should return $result for user ${user.first}") {
                    LDAPAuthenticate(fp).use {
                        lc -> lc.canUserAuthenticate(user.first, user.second)
                    } shouldEqualTo result
                }
            }
        }

        afterGroup { InMemoryLDAPServer.stop() }
    }
})
package no.nav.integrasjon.test

/*
//import io.ktor.http.HttpMethod
//import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.TestApplicationEngine
import io.ktor.server.testing.createTestEnvironment
//import io.ktor.server.testing.handleRequest
import no.nav.common.KafkaEnvironment
import no.nav.integrasjon.FasitPropFactory
import no.nav.integrasjon.FasitProperties
import no.nav.integrasjon.api.v1.ACLS
import no.nav.integrasjon.kafkaAdminREST
import no.nav.integrasjon.test.common.InMemoryLDAPServer
//import org.amshove.kluent.shouldBe
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
*/
/*
object KafkaAdminRestSpec : Spek ({

    // create and start kafka cluster
    val kCluster = KafkaEnvironment(1, autoStart = true)

    // establish correct set of fasit properties
    val fp = FasitProperties(
            kCluster.brokersURL,"kafka-adminrest","FALSE",
            "","","","",
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

    // set the correct set of fasit properties in fasit factory - before starting ktor module kafkaAdminRest
    FasitPropFactory.setFasitProperties(fp)

    given("application kafka-adminrest") {

        val engine = TestApplicationEngine(createTestEnvironment())
        engine.start(wait = false)
        engine.application.kafkaAdminREST()

        beforeGroup { InMemoryLDAPServer.start() }

        with(engine) {

            on("Route $ACLS") {

                it("should return all acls for kafka cluster") {
                    /*  Need embedded kafka with authentication and authorization - longer path...
                    with(handleRequest(HttpMethod.Get, ACLS)) {
                        response.status() shouldBe HttpStatusCode.OK
                    }
                    */
                }
            }
        }

        afterGroup {
            InMemoryLDAPServer.stop()
            kCluster.tearDown()
        }
    }
})

*/

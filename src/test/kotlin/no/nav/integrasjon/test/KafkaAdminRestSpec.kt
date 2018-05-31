package no.nav.integrasjon.test

import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import com.unboundid.ldap.sdk.ResultCode
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.TestApplicationEngine
import io.ktor.server.testing.createTestEnvironment
import io.ktor.server.testing.handleRequest
import io.ktor.server.testing.setBody
import io.ktor.util.encodeBase64
import no.nav.common.KafkaEnvironment
import no.nav.integrasjon.FasitPropFactory
import no.nav.integrasjon.FasitProperties
import no.nav.integrasjon.api.v1.ANewTopic
import no.nav.integrasjon.api.v1.BROKERS
import no.nav.integrasjon.api.v1.GROUPS
import no.nav.integrasjon.api.v1.TOPICS
import no.nav.integrasjon.kafkaAdminREST
import no.nav.integrasjon.ldap.LDAPGroup
import no.nav.integrasjon.test.common.InMemoryLDAPServer
import org.amshove.kluent.*
import org.apache.kafka.clients.admin.Config
import org.apache.kafka.clients.admin.ConfigEntry
import org.apache.kafka.clients.admin.TopicListing
import org.apache.kafka.common.Node
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on

/**
 * Since the embedded kafka doesn't support authentication&authorization yet,
 * not possible to test route ACLS or those in TOPICS (create/delete and get acls) related to acl management
 *
 * TODO - need to enhance embedded kafka
 * TODO - need to define the missing routes
 */
object KafkaAdminRestSpec : Spek ({

    // Creating topics for predefined kafka groups in LDAP
    val preTopics = setOf("tpc-01","tpc-02","tpc-03")

    // Combining srv users in ServiceAccounts and the node below, ApplAccounts (Basta)
    // to be added and removed from tpc-01
    val usersToManage = mapOf(
            "srvp01" to LDAPGroup.Companion.KafkaGroupType.PRODUCER,
            "srvc02" to LDAPGroup.Companion.KafkaGroupType.CONSUMER
    )

    val invalidTopics = mapOf(
            "invalid_test" to 1,
            "too00-lo0ng-too00-lo0ng-too00-lo0ng-too00-lo0ng-too00-lo0ng-too00-lo0ng-" to 1
    )

    // create and start kafka cluster - not sure when ktor start versus beforeGroup...
    val kCluster = KafkaEnvironment(1, topics = preTopics.toList(), autoStart = true)

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

            on("Route $BROKERS") {

                it("should return list of ${kCluster.serverPark.brokers.size} broker(s) in kafka cluster") {

                    val call = handleRequest(HttpMethod.Get, BROKERS) {
                        addHeader(HttpHeaders.Accept, "application/json")
                    }

                    val result: Collection<Node> = Gson().fromJson(
                            call.response.content ?: "",
                            object : TypeToken<Collection<Node>>() {}.type)

                    call.response.status() shouldBe HttpStatusCode.OK
                    result.size shouldEqualTo kCluster.serverPark.brokers.size
                }

                it("should return configuration for broker 0") {

                    val call = handleRequest(HttpMethod.Get, "$BROKERS/0") {
                        addHeader(HttpHeaders.Accept, "application/json")
                    }

                    val result: Map<String, Config> = Gson().fromJson(
                            call.response.content ?: "",
                            object : TypeToken<Map<String, Config>>() {}.type)

                    call.response.status() shouldBe HttpStatusCode.OK
                    result.keys.toString() shouldBeEqualTo "[ConfigResource{type=BROKER, name='0'}]"
                }
            }

            on("Route $GROUPS") {

                it("should list all kafka groups in LDAP") {

                    val call = handleRequest(HttpMethod.Get, GROUPS) {
                        addHeader(HttpHeaders.Accept, "application/json")
                    }

                    val result: Collection<String> = Gson().fromJson(
                            call.response.content ?: "",
                            object : TypeToken<Collection<String>>() {}.type)

                    call.response.status() shouldBe HttpStatusCode.OK
                    result shouldContainAll listOf("KC-tpc-01","KC-tpc-02","KC-tpc-03","KP-tpc-01","KP-tpc-02","KP-tpc-03")
                }

                val groups = mapOf(
                        "KP-tpc-01" to emptyList(),
                        "KC-tpc-02" to listOf("uid=srvc02,ou=ApplAccounts,ou=ServiceAccounts,dc=test,dc=local"),
                        "KP-tpc-03" to listOf("uid=srvp02,ou=ApplAccounts,ou=ServiceAccounts,dc=test,dc=local")
                )

                groups.forEach { group, members ->
                    it("should return $members for group $group") {

                        val call = handleRequest(HttpMethod.Get, "$GROUPS/$group") {
                            addHeader(HttpHeaders.Accept, "application/json")
                        }

                        val result: Collection<String> = Gson().fromJson(
                                call.response.content ?: "",
                                object : TypeToken<Collection<String>>() {}.type)

                        call.response.status() shouldBe HttpStatusCode.OK
                        result shouldContainAll members
                    }
                }
            }

            on("Route $TOPICS") {

                it("should list all topics $preTopics in kafka cluster") {

                    val call = handleRequest(HttpMethod.Get, TOPICS) {
                        addHeader(HttpHeaders.Accept, "application/json")
                    }

                    val result: Map<String,TopicListing> = Gson().fromJson(
                            call.response.content ?: "",
                            object : TypeToken<Map<String,TopicListing>>() {}.type)

                    call.response.status() shouldBe HttpStatusCode.OK
                    result.keys shouldContainAll preTopics
                }

                preTopics.forEach { topic ->
                    it("should return configuration for $topic") {

                        val call = handleRequest(HttpMethod.Get, "$TOPICS/$topic") {
                            addHeader(HttpHeaders.Accept, "application/json")
                        }

                        call.response.status() shouldBe HttpStatusCode.OK
                    }

                }

                it("should update 'retention.ms' configuration for tpc-03") {

                    val call = handleRequest(HttpMethod.Put, "$TOPICS/tpc-03") {
                        addHeader(HttpHeaders.Accept, "application/json")
                        addHeader(HttpHeaders.ContentType, "application/json")
                        // relevant user is in the right place in UserAndGroups.ldif
                        addHeader(
                                HttpHeaders.Authorization,
                                "Basic ${encodeBase64("iauth:itest".toByteArray())}")

                        val jsonPayload = Gson().toJson(ConfigEntry("retention.ms","6600666"))
                        setBody(jsonPayload)
                    }

                    call.response.status() shouldBe HttpStatusCode.OK
                }

                it("should return updated 'retention.ms' configuration for tpc-03") {

                    val call = handleRequest(HttpMethod.Get, "$TOPICS/tpc-03") {
                        addHeader(HttpHeaders.Accept, "application/json")
                    }

                    val result: Map<String, Config> = Gson().fromJson(
                            call.response.content ?: "",
                            object : TypeToken<Map<String, Config>>() {}.type)

                    call.response.status() shouldBe HttpStatusCode.OK
                    result.values.first()["retention.ms"].value() shouldBeEqualTo "6600666"
                }

                it("should report exception when trying to update config outside white list for tpc-03 "){

                    val call = handleRequest(HttpMethod.Put, "$TOPICS/tpc-03") {
                        addHeader(HttpHeaders.Accept, "application/json")
                        addHeader(HttpHeaders.ContentType, "application/json")
                        // relevant user is in the right place in UserAndGroups.ldif
                        addHeader(
                                HttpHeaders.Authorization,
                                "Basic ${encodeBase64("iauth:itest".toByteArray())}")

                        val jsonPayload = Gson().toJson(ConfigEntry("max.message.bytes","51000012"))
                        setBody(jsonPayload)
                    }

                    call.response.status() shouldBe HttpStatusCode.ExceptionFailed
                }

                it("should report non-existing topic for topic 'invalid'") {

                    val call = handleRequest(HttpMethod.Get, "$TOPICS/invalid") {
                        addHeader(HttpHeaders.Accept, "application/json")
                    }

                    call.response.status() shouldBe HttpStatusCode.OK
                    call.response.content.toString() shouldBeEqualTo """{"first":"Result","second":"Topic invalid does not exist"}"""
                }

                it("should report groups and members for topic tpc-03") {

                    val call = handleRequest(HttpMethod.Get, "$TOPICS/tpc-03/groups") {
                        addHeader(HttpHeaders.Accept, "application/json")
                    }

                    val result: List<LDAPGroup.Companion.KafkaGroup> = Gson().fromJson(
                            call.response.content ?: "",
                            object : TypeToken<List<LDAPGroup.Companion.KafkaGroup>>() {}.type)

                    call.response.status() shouldBe HttpStatusCode.OK
                    result.map { it.result.resultCode == ResultCode.SUCCESS } shouldEqual listOf(true,true)
                }

                usersToManage.forEach { srvUser, role ->
                    it("should add a new ${role.name} $srvUser to topic tpc-01") {

                        val call = handleRequest(HttpMethod.Put, "$TOPICS/tpc-01/groups") {
                            addHeader(HttpHeaders.Accept, "application/json")
                            addHeader(HttpHeaders.ContentType, "application/json")
                            // relevant user is in the right place in UserAndGroups.ldif
                            addHeader(
                                    HttpHeaders.Authorization,
                                    "Basic ${encodeBase64("iauth:itest".toByteArray())}")

                            val jsonPayload = Gson().toJson(
                                    LDAPGroup.Companion.UpdateKafkaGroupMember(
                                            role,
                                            LDAPGroup.Companion.GroupMemberOperation.ADD,
                                            srvUser
                                    )
                            )
                            setBody(jsonPayload)
                        }

                        call.response.status() shouldBe HttpStatusCode.OK
                    }
                }

                it("should report groups and new members for topic tpc-01") {

                    val call = handleRequest(HttpMethod.Get, "$TOPICS/tpc-01/groups") {
                        addHeader(HttpHeaders.Accept, "application/json")
                    }

                    val result: List<LDAPGroup.Companion.KafkaGroup> = Gson().fromJson(
                            call.response.content ?: "",
                            object : TypeToken<List<LDAPGroup.Companion.KafkaGroup>>() {}.type)

                    call.response.status() shouldBe HttpStatusCode.OK
                    result.map { it.result.resultCode == ResultCode.SUCCESS } shouldEqual listOf(true,true)
                    result.flatMap { it.members } shouldContainAll listOf(
                            "uid=srvc02,ou=ApplAccounts,ou=ServiceAccounts,dc=test,dc=local",
                            "uid=srvp01,ou=ServiceAccounts,dc=test,dc=local"
                    )
                }

                it("should report exception when trying to add non-existing srv user to topic tpc-01") {

                    val call = handleRequest(HttpMethod.Put, "$TOPICS/tpc-01/groups") {
                        addHeader(HttpHeaders.Accept, "application/json")
                        addHeader(HttpHeaders.ContentType, "application/json")
                        // relevant user is in the right place in UserAndGroups.ldif
                        addHeader(
                                HttpHeaders.Authorization,
                                "Basic ${encodeBase64("iauth:itest".toByteArray())}")

                        val jsonPayload = Gson().toJson(
                                LDAPGroup.Companion.UpdateKafkaGroupMember(
                                        LDAPGroup.Companion.KafkaGroupType.PRODUCER,
                                        LDAPGroup.Companion.GroupMemberOperation.ADD,
                                        "non-existing"
                                )
                        )
                        setBody(jsonPayload)
                    }

                    call.response.status() shouldBe HttpStatusCode.ExceptionFailed
                }

                usersToManage.forEach { srvUser, role ->
                    it("should remove ${role.name} member $srvUser from topic tpc-01") {

                        val call = handleRequest(HttpMethod.Put, "$TOPICS/tpc-01/groups") {
                            addHeader(HttpHeaders.Accept, "application/json")
                            addHeader(HttpHeaders.ContentType, "application/json")
                            // relevant user is in the right place in UserAndGroups.ldif
                            addHeader(
                                    HttpHeaders.Authorization,
                                    "Basic ${encodeBase64("iauth:itest".toByteArray())}")

                            val jsonPayload = Gson().toJson(
                                    LDAPGroup.Companion.UpdateKafkaGroupMember(
                                            role,
                                            LDAPGroup.Companion.GroupMemberOperation.REMOVE,
                                            srvUser
                                    )
                            )
                            setBody(jsonPayload)
                        }

                        call.response.status() shouldBe HttpStatusCode.OK
                    }
                }

                it("should report groups and 0 members for topic tpc-01") {

                    val call = handleRequest(HttpMethod.Get, "$TOPICS/tpc-01/groups") {
                        addHeader(HttpHeaders.Accept, "application/json")
                    }

                    val result: List<LDAPGroup.Companion.KafkaGroup> = Gson().fromJson(
                            call.response.content ?: "",
                            object : TypeToken<List<LDAPGroup.Companion.KafkaGroup>>() {}.type)

                    call.response.status() shouldBe HttpStatusCode.OK
                    result.map { it.result.resultCode == ResultCode.SUCCESS } shouldEqual listOf(true,true)
                    result.flatMap { it.members }.size shouldEqualTo 0
                }

                invalidTopics.forEach { topicName, numPartitions ->
                    it("should report exception when creating topic $topicName") {

                        val call = handleRequest(HttpMethod.Post, TOPICS) {
                            addHeader(HttpHeaders.Accept, "application/json")
                            addHeader(HttpHeaders.ContentType, "application/json")
                            // relevant user is in the right place in UserAndGroups.ldif
                            addHeader(
                                    HttpHeaders.Authorization,
                                    "Basic ${encodeBase64("iauth:itest".toByteArray())}")

                            val jsonPayload = Gson().toJson(ANewTopic(topicName, numPartitions))
                            setBody(jsonPayload)
                        }

                        call.response.status() shouldBe HttpStatusCode.ExceptionFailed
                    }
                }
            }
        }

        afterGroup {
            InMemoryLDAPServer.stop()
            kCluster.tearDown()
        }
    }
})



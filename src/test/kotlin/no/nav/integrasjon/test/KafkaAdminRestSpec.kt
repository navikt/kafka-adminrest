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
import no.nav.integrasjon.api.v1.AllowedConfigEntries
import no.nav.integrasjon.api.v1.BROKERS
import no.nav.integrasjon.api.v1.DeleteTopicModel
import no.nav.integrasjon.api.v1.GROUPS
import no.nav.integrasjon.api.v1.GetBrokerConfigModel
import no.nav.integrasjon.api.v1.GetBrokersModel
import no.nav.integrasjon.api.v1.GetGroupMembersModel
import no.nav.integrasjon.api.v1.GetGroupsModel
import no.nav.integrasjon.api.v1.GetTopicACLModel
import no.nav.integrasjon.api.v1.GetTopicConfigModel
import no.nav.integrasjon.api.v1.GetTopicGroupsModel
import no.nav.integrasjon.api.v1.GetTopicsModel
import no.nav.integrasjon.api.v1.ONESHOT
import no.nav.integrasjon.api.v1.OneshotCreationRequest
import no.nav.integrasjon.api.v1.PostTopicBody
import no.nav.integrasjon.api.v1.PostTopicModel
import no.nav.integrasjon.api.v1.PutTopicConfigEntryBody
import no.nav.integrasjon.api.v1.RoleMember
import no.nav.integrasjon.api.v1.TOPICS
import no.nav.integrasjon.api.v1.TopicCreation
import no.nav.integrasjon.kafkaAdminREST
import no.nav.integrasjon.ldap.GroupMemberOperation
import no.nav.integrasjon.ldap.KafkaGroupType
import no.nav.integrasjon.ldap.UpdateKafkaGroupMember
import no.nav.integrasjon.test.common.InMemoryLDAPServer
import org.amshove.kluent.shouldBe
import org.amshove.kluent.shouldBeEqualTo
import org.amshove.kluent.shouldContain
import org.amshove.kluent.shouldContainAll
import org.amshove.kluent.shouldEqual
import org.amshove.kluent.shouldEqualTo
import org.apache.kafka.clients.admin.ConfigEntry
import org.spekframework.spek2.Spek
import org.spekframework.spek2.style.specification.describe

object KafkaAdminRestSpec : Spek({

    // Creating topics for predefined kafka groups in LDAP
    val preTopics = setOf("tpc-01", "tpc-02", "tpc-03")

    // topics to create in tests
    val topics2CreateDelete = listOf("tpc-created01", "tpc-created02", "tpc-created03")
    val topics4ACLTesting = listOf("tpc-acl01")

    // Combining srv users in ServiceAccounts and the node below, ApplAccounts (Basta)
    // to be added and removed from tpc-01
    val usersToManage = mapOf(
        "srvp01" to KafkaGroupType.PRODUCER,
        "srvc02" to KafkaGroupType.CONSUMER,
        "n145821" to KafkaGroupType.MANAGER
    )

    val invalidTopics = mapOf(
            "invalid_test" to 1,
            "too00-lo0ng-too00-lo0ng-too00-lo0ng-too00-lo0ng-too00-lo0ng-too00-lo0ng-" to 1
    )

    // create and start kafka cluster - not sure when ktor start versus beforeGroup...
    val kCluster = KafkaEnvironment(1, topics = preTopics.toList(), withSecurity = true, autoStart = true)

    // establish correct set of fasit properties
    val fp = FasitProperties(
            kCluster.brokersURL, "kafka-adminrest", "TRUE",
            "SASL_PLAINTEXT", "PLAIN",
            "srvkafkaclient", "kafkaclient", // see predfined users in embedded kafka
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

    describe("application kafka-adminrest") {

        val engine = TestApplicationEngine(createTestEnvironment())
        engine.start(wait = false)
        engine.application.kafkaAdminREST()

        beforeGroup { InMemoryLDAPServer.start() }

        with(engine) {

            context("Route $BROKERS") {

                it("should return list of ${kCluster.brokers.size} broker(s) in kafka cluster") {

                    val call = handleRequest(HttpMethod.Get, BROKERS) {
                        addHeader(HttpHeaders.Accept, "application/json")
                    }

                    val result: GetBrokersModel = Gson().fromJson(
                            call.response.content ?: "",
                            object : TypeToken<GetBrokersModel>() {}.type)

                    call.response.status() shouldBe HttpStatusCode.OK
                    result.brokers.size shouldEqualTo kCluster.brokers.size
                }

                it("should return configuration for broker 0") {

                    val call = handleRequest(HttpMethod.Get, "$BROKERS/0") {
                        addHeader(HttpHeaders.Accept, "application/json")
                    }

                    val result: GetBrokerConfigModel = Gson().fromJson(
                            call.response.content ?: "",
                            object : TypeToken<GetBrokerConfigModel>() {}.type)

                    call.response.status() shouldBe HttpStatusCode.OK
                    result.id shouldBeEqualTo "0"
                }
            }

            context("Route $GROUPS") {

                it("should list all kafka groups in LDAP") {

                    val call = handleRequest(HttpMethod.Get, GROUPS) {
                        addHeader(HttpHeaders.Accept, "application/json")
                    }

                    val result: GetGroupsModel = Gson().fromJson(
                            call.response.content ?: "",
                            object : TypeToken<GetGroupsModel>() {}.type)

                    call.response.status() shouldBe HttpStatusCode.OK
                    result.groups shouldContainAll listOf("KC-tpc-01", "KC-tpc-02", "KC-tpc-03", "KP-tpc-01", "KP-tpc-02", "KP-tpc-03")
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

                        val result: GetGroupMembersModel = Gson().fromJson(
                                call.response.content ?: "",
                                object : TypeToken<GetGroupMembersModel>() {}.type)

                        call.response.status() shouldBe HttpStatusCode.OK
                        result.members shouldContainAll members
                    }
                }
            }

            context("Route $TOPICS") {

                it("should list all topics $preTopics in kafka cluster") {

                    val call = handleRequest(HttpMethod.Get, TOPICS) {
                        addHeader(HttpHeaders.Accept, "application/json")
                    }

                    val result: GetTopicsModel = Gson().fromJson(
                            call.response.content ?: "",
                            object : TypeToken<GetTopicsModel>() {}.type)

                    call.response.status() shouldBe HttpStatusCode.OK
                    result.topics shouldContainAll preTopics
                }

                (topics2CreateDelete + topics4ACLTesting).forEach { topicToCreate ->

                    it("should create topic $topicToCreate") {

                        val call = handleRequest(HttpMethod.Post, TOPICS) {
                            addHeader(HttpHeaders.Accept, "application/json")
                            addHeader(HttpHeaders.ContentType, "application/json")
                            addHeader(HttpHeaders.Authorization, "Basic ${encodeBase64("n000002:itest2".toByteArray())}")
                            setBody(Gson().toJson(PostTopicBody(topicToCreate)))
                        }

                        val result: PostTopicModel = Gson().fromJson(
                                call.response.content ?: "",
                                object : TypeToken<PostTopicModel>() {}.type)

                        call.response.status() shouldBe HttpStatusCode.OK

                        result.topicStatus shouldContain "created topic"
                        result.groupsStatus.map { it.ldapResult.resultCode.name } shouldContainAll listOf("success", "success", "success")
                        result.aclStatus shouldContain "created"
                    }
                }

                it("should not be possible to delete ${topics2CreateDelete.first()} for non-member in KM-") {
                    val call = handleRequest(HttpMethod.Delete, "$TOPICS/${topics2CreateDelete.first()}") {
                        addHeader(HttpHeaders.Accept, "application/json")
                        addHeader(HttpHeaders.ContentType, "application/json")
                        addHeader(HttpHeaders.Authorization, "Basic ${encodeBase64("igroup:itest".toByteArray())}")
                    }

                    call.response.status() shouldBe HttpStatusCode.Unauthorized
                }

                topics2CreateDelete.forEach { topicToDelete ->

                    it("should delete topic $topicToDelete for member in KM-") {

                        val call = handleRequest(HttpMethod.Delete, "$TOPICS/$topicToDelete") {
                            addHeader(HttpHeaders.Accept, "application/json")
                            addHeader(HttpHeaders.ContentType, "application/json")
                            addHeader(HttpHeaders.Authorization, "Basic ${encodeBase64("n000002:itest2".toByteArray())}")
                        }

                        val result: DeleteTopicModel = Gson().fromJson(
                                call.response.content ?: "",
                                object : TypeToken<DeleteTopicModel>() {}.type)

                        call.response.status() shouldBe HttpStatusCode.OK

                        result.topicStatus shouldContain "deleted topic"
                        result.groupsStatus.map { it.ldapResult.resultCode.name } shouldContainAll listOf("success", "success", "success")
                        result.aclStatus shouldContain "deleted"
                    }
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
                                "Basic ${encodeBase64("n145821:itest3".toByteArray())}")

                        val jsonPayload = Gson().toJson(
                                PutTopicConfigEntryBody(AllowedConfigEntries.RETENTION_BYTES, "6600666"))
                        setBody(jsonPayload)
                    }

                    call.response.status() shouldBe HttpStatusCode.OK
                }

                it("should return updated 'retention.ms' configuration for tpc-03") {

                    val call = handleRequest(HttpMethod.Get, "$TOPICS/tpc-03") {
                        addHeader(HttpHeaders.Accept, "application/json")
                    }

                    val result: GetTopicConfigModel = Gson().fromJson(
                            call.response.content ?: "",
                            object : TypeToken<GetTopicConfigModel>() {}.type)

                    call.response.status() shouldBe HttpStatusCode.OK
                    result.config.find { it.name() == "retention.ms" }?.value() ?: "" shouldBeEqualTo "6600666"
                }

                it("should report exception when trying to update config outside white list for tpc-03 ") {

                    val call = handleRequest(HttpMethod.Put, "$TOPICS/tpc-03") {
                        addHeader(HttpHeaders.Accept, "application/json")
                        addHeader(HttpHeaders.ContentType, "application/json")
                        // relevant user is in the right place in UserAndGroups.ldif
                        addHeader(
                                HttpHeaders.Authorization,
                                "Basic ${encodeBase64("N145821:itest3".toByteArray())}")

                        val jsonPayload = Gson().toJson(ConfigEntry("max.message.bytes", "51000012"))
                        setBody(jsonPayload)
                    }

                    call.response.status() shouldBe HttpStatusCode.ExpectationFailed
                }

                topics4ACLTesting.forEach { tpcACL ->
                    it("should for topic $tpcACL report standard ACL for KP- and KC- group") {
                        val call = handleRequest(HttpMethod.Get, "$TOPICS/$tpcACL/acls") {
                            addHeader(HttpHeaders.Accept, "application/json")
                            addHeader(HttpHeaders.ContentType, "application/json")
                            addHeader(HttpHeaders.Authorization, "Basic ${encodeBase64("n000002:itest2".toByteArray())}")
                        }

                        val result: GetTopicACLModel = Gson().fromJson(
                                call.response.content ?: "",
                                object : TypeToken<GetTopicACLModel>() {}.type)

                        call.response.status() shouldBe HttpStatusCode.OK
                    }
                }

                it("should report non-existing topic for topic 'invalid'") {

                    val call = handleRequest(HttpMethod.Get, "$TOPICS/invalid") {
                        addHeader(HttpHeaders.Accept, "application/json")
                    }

                    call.response.status() shouldBe HttpStatusCode.ExpectationFailed
                    call.response.content.toString() shouldBeEqualTo """{"error":"Sorry, exception happened - java.lang.Exception: failure, topic invalid does not exist"}"""
                }

                it("should report groups and members for topic tpc-03") {

                    val call = handleRequest(HttpMethod.Get, "$TOPICS/tpc-03/groups") {
                        addHeader(HttpHeaders.Accept, "application/json")
                    }

                    val result: GetTopicGroupsModel = Gson().fromJson(
                            call.response.content ?: "",
                            object : TypeToken<GetTopicGroupsModel>() {}.type)

                    call.response.status() shouldBe HttpStatusCode.OK
                    result.groups.map { it.ldapResult.resultCode == ResultCode.SUCCESS } shouldEqual listOf(true, true, true)
                }

                usersToManage.forEach { srvUser, role ->
                    it("should add a new ${role.name} $srvUser to topic tpc-01") {

                        val call = handleRequest(HttpMethod.Put, "$TOPICS/tpc-01/groups") {
                            addHeader(HttpHeaders.Accept, "application/json")
                            addHeader(HttpHeaders.ContentType, "application/json")
                            // relevant user is in the right place in UserAndGroups.ldif
                            addHeader(
                                    HttpHeaders.Authorization,
                                    "Basic ${encodeBase64("n000002:itest2".toByteArray())}")

                            val jsonPayload = Gson().toJson(
                                    UpdateKafkaGroupMember(
                                            role,
                                            GroupMemberOperation.ADD,
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

                    val result: GetTopicGroupsModel = Gson().fromJson(
                            call.response.content ?: "",
                            object : TypeToken<GetTopicGroupsModel>() {}.type)

                    call.response.status() shouldBe HttpStatusCode.OK
                    result.groups.map { it.ldapResult.resultCode == ResultCode.SUCCESS } shouldEqual listOf(true, true, true)
                    result.groups.flatMap { it.members } shouldContainAll listOf(
                            "uid=srvc02,ou=ApplAccounts,ou=ServiceAccounts,dc=test,dc=local",
                            "uid=srvp01,ou=ServiceAccounts,dc=test,dc=local",
                            "uid=n145821,ou=Users,ou=NAV,ou=BusinessUnits,dc=test,dc=local",
                            "uid=n000002,ou=Users,ou=NAV,ou=BusinessUnits,dc=test,dc=local"
                    )
                }

                it("should report exception when trying to add non-existing srv user to topic tpc-01") {

                    val call = handleRequest(HttpMethod.Put, "$TOPICS/tpc-01/groups") {
                        addHeader(HttpHeaders.Accept, "application/json")
                        addHeader(HttpHeaders.ContentType, "application/json")
                        // relevant user is in the right place in UserAndGroups.ldif
                        addHeader(
                                HttpHeaders.Authorization,
                                "Basic ${encodeBase64("n000002:itest2".toByteArray())}")

                        val jsonPayload = Gson().toJson(
                                UpdateKafkaGroupMember(
                                        KafkaGroupType.PRODUCER,
                                        GroupMemberOperation.ADD,
                                        "non-existing"
                                )
                        )
                        setBody(jsonPayload)
                    }

                    call.response.status() shouldBe HttpStatusCode.ExpectationFailed
                }

                usersToManage.forEach { srvUser, role ->
                    it("should remove ${role.name} member $srvUser from topic tpc-01") {

                        val call = handleRequest(HttpMethod.Put, "$TOPICS/tpc-01/groups") {
                            addHeader(HttpHeaders.Accept, "application/json")
                            addHeader(HttpHeaders.ContentType, "application/json")
                            // relevant user is in the right place in UserAndGroups.ldif
                            addHeader(
                                    HttpHeaders.Authorization,
                                    "Basic ${encodeBase64("n000002:itest2".toByteArray())}")

                            val jsonPayload = Gson().toJson(
                                    UpdateKafkaGroupMember(
                                            role,
                                            GroupMemberOperation.REMOVE,
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

                    val result: GetTopicGroupsModel = Gson().fromJson(
                            call.response.content ?: "",
                            object : TypeToken<GetTopicGroupsModel>() {}.type)

                    call.response.status() shouldBe HttpStatusCode.OK
                    result.groups.map { it.ldapResult.resultCode == ResultCode.SUCCESS } shouldEqual listOf(true, true, true)
                    result.groups.flatMap { it.members }.size shouldEqualTo 1
                }

                invalidTopics.forEach { topicName, numPartitions ->
                    it("should report exception when creating topic $topicName") {

                        val call = handleRequest(HttpMethod.Post, TOPICS) {
                            addHeader(HttpHeaders.Accept, "application/json")
                            addHeader(HttpHeaders.ContentType, "application/json")
                            // relevant user is in the right place in UserAndGroups.ldif
                            addHeader(
                                    HttpHeaders.Authorization,
                                    "Basic ${encodeBase64("srvp01:dummy".toByteArray())}")

                            val jsonPayload = Gson().toJson(PostTopicBody(topicName, numPartitions))
                            setBody(jsonPayload)
                        }

                        call.response.status() shouldBe HttpStatusCode.ExpectationFailed
                    }
                }
            }

            context("Route $ONESHOT") {
                it("creates a topic with one consumer + manager") {
                    val call = handleRequest(HttpMethod.Put, ONESHOT) {
                        addHeader(HttpHeaders.Accept, "application/json")
                        addHeader(HttpHeaders.ContentType, "application/json")
                        addHeader(HttpHeaders.Authorization, "Basic ${encodeBase64("igroup:itest".toByteArray())}")
                        setBody(Gson().toJson(OneshotCreationRequest(
                                topics = listOf(
                                        TopicCreation(
                                                topicName = "integrationTestNoUpdate",
                                                members = listOf(RoleMember("srvp01", KafkaGroupType.CONSUMER)),
                                                configEntries = mapOf(),
                                                numPartitions = 3
                                        )))))
                    }

                    println(call.response.content)
                    call.response.status() shouldBe HttpStatusCode.OK
                }
            }
        }

        afterGroup {
            InMemoryLDAPServer.stop()
            kCluster.tearDown()
        }
    }
})

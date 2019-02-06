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
import no.nav.common.KafkaEnvironment
import no.nav.integrasjon.FasitPropFactory
import no.nav.integrasjon.FasitProperties
import no.nav.integrasjon.api.nais.client.SERVICES_ERR_A
import no.nav.integrasjon.api.nais.client.SERVICES_ERR_G
import no.nav.integrasjon.api.nais.client.SERVICES_ERR_GAK
import no.nav.integrasjon.api.nais.client.SERVICES_ERR_K
import no.nav.integrasjon.api.v1.ACLS
import no.nav.integrasjon.api.v1.APIGW
import no.nav.integrasjon.api.v1.AllowedConfigEntries
import no.nav.integrasjon.api.v1.AnError
import no.nav.integrasjon.api.v1.ApiGwGroupMember
import no.nav.integrasjon.api.v1.ApiGwRequest
import no.nav.integrasjon.api.v1.BROKERS
import no.nav.integrasjon.api.v1.DeleteTopicModel
import no.nav.integrasjon.api.v1.GROUPS
import no.nav.integrasjon.api.v1.GetApiGwGroupMembersModel
import no.nav.integrasjon.api.v1.GetBrokerConfigModel
import no.nav.integrasjon.api.v1.GetBrokersModel
import no.nav.integrasjon.api.v1.GetGroupMembersModel
import no.nav.integrasjon.api.v1.GetGroupsModel
import no.nav.integrasjon.api.v1.GetTopicACLModel
import no.nav.integrasjon.api.v1.GetTopicConfigModel
import no.nav.integrasjon.api.v1.GetTopicGroupsModel
import no.nav.integrasjon.api.v1.GetTopicsModel
import no.nav.integrasjon.api.v1.NAIS_ISALIVE
import no.nav.integrasjon.api.v1.NAIS_ISREADY
import no.nav.integrasjon.api.v1.ONESHOT
import no.nav.integrasjon.api.v1.OneshotCreationRequest
import no.nav.integrasjon.api.v1.PostStreamBody
import no.nav.integrasjon.api.v1.PostStreamResponse
import no.nav.integrasjon.api.v1.PostStreamStatus
import no.nav.integrasjon.api.v1.PostTopicBody
import no.nav.integrasjon.api.v1.PostTopicModel
import no.nav.integrasjon.api.v1.PutApiGwResultModel
import no.nav.integrasjon.api.v1.PutTopicConfigEntryBody
import no.nav.integrasjon.api.v1.PutTopicGMemberModel
import no.nav.integrasjon.api.v1.RoleMember
import no.nav.integrasjon.api.v1.STREAMS
import no.nav.integrasjon.api.v1.TOPICS
import no.nav.integrasjon.api.v1.TopicCreation
import no.nav.integrasjon.kafkaAdminREST
import no.nav.integrasjon.ldap.GroupMemberOperation
import no.nav.integrasjon.ldap.KafkaGroupType
import no.nav.integrasjon.ldap.UpdateKafkaGroupMember
import no.nav.integrasjon.ldap.intoAcls
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
import java.util.Base64
import java.util.concurrent.TimeUnit

object KafkaAdminRestSpec : Spek({

    // Creating topics for predefined kafka groups in LDAP
    val preTopics = setOf("tpc-01", "tpc-02", "tpc-03")

    // create and start kafka cluster - not sure when ktor start versus beforeGroup...
    val kCluster = KafkaEnvironment(1, topics = preTopics.toList(), withSecurity = true, autoStart = true)

    val correctFP = FasitProperties(
        kCluster.brokersURL, "kafka-adminrest", "TRUE",
        "SASL_PLAINTEXT", "PLAIN",
        "srvkafkaclient", "kafkaclient", // see predfined users in embedded kafka
        ldapConnTimeout = 1000,
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
        kafkaTimeout = 1000L,
        ldapGroupInGroupBase = "OU=Groups,OU=NAV,OU=BusinessUnits,DC=test,DC=local"
    )

    fun FasitProperties.injectValues(
        portLDAPGroup: Int = InMemoryLDAPServer.LPORT,
        portLDAPAuth: Int = InMemoryLDAPServer.LPORT,
        kafkaURL: String = kCluster.brokersURL
    ) =
        FasitProperties(
            kafkaURL,
            kafkaClientID,
            kafkaSecurity,
            kafkaSecProt,
            kafkaSaslMec,
            kafkaUser,
            kafkaPassword,
            kafkaTimeout,

            ldapConnTimeout,
            ldapUserAttrName,

            ldapAuthHost,
            portLDAPAuth,
            ldapAuthUserBase,

            ldapHost,
            portLDAPGroup,

            ldapSrvUserBase,
            ldapGroupBase,
            ldapGroupAttrName,
            ldapGrpMemberAttrName,

            ldapUser,
            ldapPassword
        )

    describe("Test of different services down, and all services up (ldap auth and group, and kafka)") {

        beforeGroup { InMemoryLDAPServer.start() }

        context("test of different services down (ldap auth and group, and kafka)") {

            data class Scenario(
                val method: HttpMethod,
                val route: String,
                val body: String = "",
                val security: Boolean = false,
                val response: HttpStatusCode
            )

            data class ServiceDown(
                val error: String,
                val fasitProps: FasitProperties,
                val scenarios: List<Scenario>,
                val details: String = ""
            )

            // all endpoints with authentication will get unauthorized due to ldap auth not available

            val allDownServices = listOf(
                Scenario(HttpMethod.Get, ACLS, security = true, response = HttpStatusCode.Unauthorized),
                Scenario(HttpMethod.Get, BROKERS, response = HttpStatusCode.ServiceUnavailable),
                Scenario(HttpMethod.Get, "$BROKERS/0", response = HttpStatusCode.ServiceUnavailable),
                Scenario(HttpMethod.Get, GROUPS, response = HttpStatusCode.ServiceUnavailable),
                Scenario(HttpMethod.Get, "$GROUPS/tpc-02", response = HttpStatusCode.ServiceUnavailable),
                Scenario(HttpMethod.Get, TOPICS, response = HttpStatusCode.ServiceUnavailable),
                Scenario(
                    HttpMethod.Post,
                    TOPICS,
                    body = Gson().toJson(PostTopicBody("tpc-alldown")),
                    security = true,
                    response = HttpStatusCode.Unauthorized),
                Scenario(HttpMethod.Get, "$TOPICS/tpc-02", response = HttpStatusCode.ServiceUnavailable),
                Scenario(HttpMethod.Get, "$TOPICS/tpc-02/acls", response = HttpStatusCode.ServiceUnavailable),
                Scenario(HttpMethod.Get, "$TOPICS/tpc-02/groups", response = HttpStatusCode.ServiceUnavailable)
            )

            val kafkaDownScenarios = listOf(
                Scenario(HttpMethod.Get, ACLS, security = true, response = HttpStatusCode.ServiceUnavailable),
                Scenario(HttpMethod.Get, BROKERS, response = HttpStatusCode.ServiceUnavailable),
                Scenario(HttpMethod.Get, "$BROKERS/0", response = HttpStatusCode.ServiceUnavailable),
                Scenario(HttpMethod.Get, GROUPS, response = HttpStatusCode.OK),
                Scenario(HttpMethod.Get, "$GROUPS/tpc-02", response = HttpStatusCode.OK),
                Scenario(HttpMethod.Get, TOPICS, response = HttpStatusCode.ServiceUnavailable),
                Scenario(
                    HttpMethod.Post,
                    TOPICS,
                    body = Gson().toJson(PostTopicBody("tpc-alldown")),
                    security = true,
                    response = HttpStatusCode.ServiceUnavailable),
                Scenario(HttpMethod.Get, "$TOPICS/tpc-02", response = HttpStatusCode.ServiceUnavailable),
                Scenario(HttpMethod.Get, "$TOPICS/tpc-02/acls", response = HttpStatusCode.ServiceUnavailable),
                Scenario(HttpMethod.Get, "$TOPICS/tpc-02/groups", response = HttpStatusCode.OK)
            )

            val ldapGroupDown = listOf(
                Scenario(HttpMethod.Get, ACLS, security = true, response = HttpStatusCode.OK),
                Scenario(HttpMethod.Get, BROKERS, response = HttpStatusCode.OK),
                Scenario(HttpMethod.Get, "$BROKERS/0", response = HttpStatusCode.OK),
                Scenario(HttpMethod.Get, GROUPS, response = HttpStatusCode.ServiceUnavailable),
                Scenario(HttpMethod.Get, "$GROUPS/tpc-02", response = HttpStatusCode.ServiceUnavailable),
                Scenario(HttpMethod.Get, TOPICS, response = HttpStatusCode.OK),
                Scenario(
                    HttpMethod.Post,
                    TOPICS,
                    body = Gson().toJson(PostTopicBody("tpc-alldown")),
                    security = true,
                    response = HttpStatusCode.ServiceUnavailable),
                Scenario(HttpMethod.Get, "$TOPICS/tpc-02", response = HttpStatusCode.OK),
                Scenario(HttpMethod.Get, "$TOPICS/tpc-02/acls", response = HttpStatusCode.OK),
                Scenario(HttpMethod.Get, "$TOPICS/tpc-02/groups", response = HttpStatusCode.ServiceUnavailable)
            )

            val ldapAuthDown = listOf(
                Scenario(HttpMethod.Get, ACLS, security = true, response = HttpStatusCode.Unauthorized),
                Scenario(HttpMethod.Get, BROKERS, response = HttpStatusCode.OK),
                Scenario(HttpMethod.Get, "$BROKERS/0", response = HttpStatusCode.OK),
                Scenario(HttpMethod.Get, GROUPS, response = HttpStatusCode.OK),
                Scenario(HttpMethod.Get, "$GROUPS/tpc-02", response = HttpStatusCode.OK),
                Scenario(HttpMethod.Get, TOPICS, response = HttpStatusCode.OK),
                Scenario(
                    HttpMethod.Post,
                    TOPICS,
                    body = Gson().toJson(PostTopicBody("tpc-alldown")),
                    security = true,
                    response = HttpStatusCode.Unauthorized),
                Scenario(HttpMethod.Get, "$TOPICS/tpc-02", response = HttpStatusCode.OK),
                Scenario(HttpMethod.Get, "$TOPICS/tpc-02/acls", response = HttpStatusCode.OK),
                Scenario(HttpMethod.Get, "$TOPICS/tpc-02/groups", response = HttpStatusCode.OK)
            )

            val srvsDown = listOf(
                ServiceDown(
                    SERVICES_ERR_GAK,
                    correctFP.injectValues(0, 0, "Wrong_Broker_URL"),
                    allDownServices
                ),
                ServiceDown(
                    SERVICES_ERR_A,
                    correctFP.injectValues(portLDAPAuth = 0),
                    ldapAuthDown
                ),
                ServiceDown(
                    SERVICES_ERR_G,
                    correctFP.injectValues(portLDAPGroup = 0),
                    ldapGroupDown
                ),
                ServiceDown(
                    SERVICES_ERR_K,
                    correctFP.injectValues(kafkaURL = "Wrong_Broker_URL"),
                    kafkaDownScenarios,
                    "invalid broker url"
                ),
                ServiceDown(
                    SERVICES_ERR_K,
                    correctFP.injectValues(kafkaURL = "SASL_PLAINTEXT://localhost:01"),
                    kafkaDownScenarios,
                    "wrong broker port"
                )
            )

            srvsDown.forEach { srvDown ->

                context("${srvDown.error} - ${srvDown.details}") {

                    val engine = TestApplicationEngine(createTestEnvironment())

                    beforeGroup {
                        FasitPropFactory.setFasitProperties(srvDown.fasitProps)
                        engine.start(wait = false)
                        engine.application.kafkaAdminREST()
                    }

                    with(engine) {

                        context("NAIS API") {
                            it("should return OK for isAlive") {
                                val call = handleRequest(HttpMethod.Get, NAIS_ISALIVE) {
                                    addHeader(HttpHeaders.Accept, "application/json")
                                }

                                call.response.status() shouldBe HttpStatusCode.OK
                            }

                            it("should return ${srvDown.error}") {
                                val call = handleRequest(HttpMethod.Get, NAIS_ISREADY) {
                                    addHeader(HttpHeaders.Accept, "application/json")
                                }

                                val result: AnError = Gson().fromJson(
                                    call.response.content ?: "",
                                    object : TypeToken<AnError>() {}.type)

                                call.response.status() shouldBe HttpStatusCode.ServiceUnavailable
                                result.error shouldBeEqualTo srvDown.error
                            }
                        }

                        context("Routes") {
                            srvDown.scenarios.forEach { scenario ->

                                it("Route ${scenario.route} should return ${scenario.response}") {

                                    val call = handleRequest(scenario.method, scenario.route) {
                                        addHeader(HttpHeaders.Accept, "application/json")
                                        addHeader(HttpHeaders.ContentType, "application/json")
                                        addHeader(
                                            HttpHeaders.Authorization,
                                            "Basic ${encodeBase64("n000002:itest2".toByteArray())}"
                                        )
                                        setBody(scenario.body)
                                    }

                                    call.response.status() shouldBe scenario.response
                                }
                            }
                        }
                    }

                    afterGroup {
                        engine.stop(1000, 2000, TimeUnit.MILLISECONDS)
                    }
                }
            }
        }

        context("test of all backend services up (ldap auth and group, and kafka)") {

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

            val engine2 = TestApplicationEngine(createTestEnvironment())

            beforeGroup {
                InMemoryLDAPServer.start()
                FasitPropFactory.setFasitProperties(correctFP)
                engine2.start(wait = false)
                engine2.application.kafkaAdminREST()
            }

            with(engine2) {

                context("Route $ACLS") {
                    // don't bother :-)
                }

                context("Route $STREAMS") {
                    context("Create streams app ACLs") {
                        it("should create ACL for streams app") {
                            val call = handleRequest(HttpMethod.Post, "$STREAMS/") {
                                addHeader(HttpHeaders.Accept, "application/json")
                                addHeader(HttpHeaders.ContentType, "application/json")
                                addHeader(HttpHeaders.Authorization, "Basic ${encodeBase64("n000002:itest2".toByteArray())}")
                                setBody(Gson().toJson(PostStreamBody("team1-streams-app1", "team1")))
                            }

                            println(call.response.content)

                            val result: PostStreamResponse = Gson().fromJson(
                                    call.response.content ?: "",
                                    object : TypeToken<PostStreamResponse>() {}.type)

                            call.response.status() shouldBe HttpStatusCode.OK
                            result.status shouldBe PostStreamStatus.OK
                        }
                    }
                }

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

                    context("Create topics") {

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

                        invalidTopics.forEach { topicName, numPartitions ->
                            it("should report bad request when creating topic $topicName") {

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

                                call.response.status() shouldBe HttpStatusCode.BadRequest
                            }
                        }
                    }

                    context("Get topics") {

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
                    }

                    context("Get/update topic configuration") {

                        preTopics.forEach { topic ->
                            it("should return configuration for $topic") {

                                val call = handleRequest(HttpMethod.Get, "$TOPICS/$topic") {
                                    addHeader(HttpHeaders.Accept, "application/json")
                                }

                                call.response.status() shouldBe HttpStatusCode.OK
                            }
                        }

                        it("should return bad request for non-existing topic 'donotexist'") {

                            val call = handleRequest(HttpMethod.Get, "$TOPICS/donotexist") {
                                addHeader(HttpHeaders.Accept, "application/json")
                            }

                            call.response.status() shouldBe HttpStatusCode.BadRequest
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

                        it("should update 'delete.retention.ms' configuration for tpc-03") {

                            val call = handleRequest(HttpMethod.Put, "$TOPICS/tpc-03") {
                                addHeader(HttpHeaders.Accept, "application/json")
                                addHeader(HttpHeaders.ContentType, "application/json")
                                // relevant user is in the right place in UserAndGroups.ldif
                                addHeader(
                                    HttpHeaders.Authorization,
                                    "Basic ${encodeBase64("n145821:itest3".toByteArray())}")

                                val jsonPayload = Gson().toJson(
                                    PutTopicConfigEntryBody(AllowedConfigEntries.DELETE_RETENTION_MS, "6600666"))
                                setBody(jsonPayload)
                            }

                            call.response.status() shouldBe HttpStatusCode.OK
                        }

                        it("should return updated 'delete.retention.ms' configuration for tpc-03") {

                            val call = handleRequest(HttpMethod.Get, "$TOPICS/tpc-03") {
                                addHeader(HttpHeaders.Accept, "application/json")
                            }

                            val result: GetTopicConfigModel = Gson().fromJson(
                                call.response.content ?: "",
                                object : TypeToken<GetTopicConfigModel>() {}.type)

                            call.response.status() shouldBe HttpStatusCode.OK
                            result.config.find { it.name() == "delete.retention.ms" }?.value() ?: "" shouldBeEqualTo "6600666"
                        }

                        it("should report bad request when trying to update config outside white list for tpc-03 ") {

                            val call = handleRequest(HttpMethod.Put, "$TOPICS/tpc-03") {
                                addHeader(HttpHeaders.Accept, "application/json")
                                addHeader(HttpHeaders.ContentType, "application/json")
                                // relevant user is in the right place in UserAndGroups.ldif
                                addHeader(
                                    HttpHeaders.Authorization,
                                    "Basic ${encodeBase64("N145821:itest3".toByteArray())}")

                                val jsonPayload = Gson().toJson(
                                    ConfigEntry("max.message.bytes", "51000012")
                                )
                                setBody(jsonPayload)
                            }

                            call.response.status() shouldBe HttpStatusCode.BadRequest
                        }
                    }

                    context("Delete topics") {

                        it("should not be possible to delete ${topics2CreateDelete.first()} for non-member in KM-") {
                            val call = handleRequest(HttpMethod.Delete, "$TOPICS/${topics2CreateDelete.first()}") {
                                addHeader(HttpHeaders.Accept, "application/json")
                                addHeader(HttpHeaders.ContentType, "application/json")
                                addHeader(HttpHeaders.Authorization, "Basic ${encodeBase64("igroup:itest".toByteArray())}")
                            }

                            call.response.status() shouldBe HttpStatusCode.BadRequest
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
                    }

                    context("Get topic ACLs") {

                        topics4ACLTesting.forEach { tpcACL ->
                            it("should for topic $tpcACL report standard ACL for KP- and KC- groups") {
                                val call = handleRequest(HttpMethod.Get, "$TOPICS/$tpcACL/acls") {
                                    addHeader(HttpHeaders.Accept, "application/json")
                                    addHeader(HttpHeaders.ContentType, "application/json")
                                    addHeader(HttpHeaders.Authorization, "Basic ${encodeBase64("n000002:itest2".toByteArray())}")
                                }

                                val result: GetTopicACLModel = Gson().fromJson(
                                    call.response.content ?: "",
                                    object : TypeToken<GetTopicACLModel>() {}.type)

                                val expectedResult = KafkaGroupType.values()
                                    .filter { it != KafkaGroupType.MANAGER }
                                    .map { grType -> grType.intoAcls(tpcACL) }
                                    .flatten()

                                call.response.status() shouldBe HttpStatusCode.OK
                                result.acls shouldContainAll expectedResult
                            }
                        }
                    }

                    context("Get/update topic groups") {

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

                        it("should report group and GroupInGroupMembers tpc-01") {

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
                                "uid=n000002,ou=Users,ou=NAV,ou=BusinessUnits,dc=test,dc=local",
                                "cn=Group_00020ec3-6592-4415-a563-1ed6768d6086,OU=O365Groups,OU=Groups,OU=NAV,OU=BusinessUnits,DC=test,DC=local"
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

                            call.response.status() shouldBe HttpStatusCode.ServiceUnavailable
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

                        it("should report groups and 2 members for topic tpc-01") {

                            val call = handleRequest(HttpMethod.Get, "$TOPICS/tpc-01/groups") {
                                addHeader(HttpHeaders.Accept, "application/json")
                            }

                            val result: GetTopicGroupsModel = Gson().fromJson(
                                call.response.content ?: "",
                                object : TypeToken<GetTopicGroupsModel>() {}.type)

                            call.response.status() shouldBe HttpStatusCode.OK
                            result.groups.map { it.ldapResult.resultCode == ResultCode.SUCCESS } shouldEqual listOf(true, true, true)
                            result.groups.flatMap { it.members }.size shouldEqualTo 2
                            result.groups.flatMap { it.members } shouldContainAll listOf(
                                "uid=n000002,ou=Users,ou=NAV,ou=BusinessUnits,dc=test,dc=local",
                                "cn=Group_00020ec3-6592-4415-a563-1ed6768d6086,OU=O365Groups,OU=Groups,OU=NAV,OU=BusinessUnits,DC=test,DC=local")
                        }
                    }
                }

                context("Route $ONESHOT") {
                    it("should successfully create a topic for duplicate combinations of roles and members (including both implicit and explicit MANAGER)") {
                        val call = handleRequest(HttpMethod.Put, ONESHOT) {
                            addHeader(HttpHeaders.Accept, "application/json")
                            addHeader(HttpHeaders.ContentType, "application/json")
                            addHeader(HttpHeaders.Authorization, "Basic ${encodeBase64("igroup:itest".toByteArray())}")
                            setBody(Gson().toJson(OneshotCreationRequest(
                                topics = listOf(
                                    TopicCreation(
                                        topicName = "integrationTestNoUpdate",
                                        members = listOf(
                                            RoleMember("srvp02", KafkaGroupType.CONSUMER),
                                            RoleMember("igroup", KafkaGroupType.MANAGER),
                                            RoleMember("igroup", KafkaGroupType.PRODUCER),
                                            RoleMember("igroup", KafkaGroupType.PRODUCER),
                                            RoleMember("igroup", KafkaGroupType.CONSUMER),
                                            RoleMember("igroup", KafkaGroupType.CONSUMER)),
                                        configEntries = mapOf(),
                                        numPartitions = 3
                                    )))))
                        }
                        call.response.status() shouldBe HttpStatusCode.OK
                    }

                    it("should report groups and 2 members for topic integrationTestNoUpdate, KM:igroup and KC:srvp02") {

                        val call = handleRequest(HttpMethod.Get, "$TOPICS/integrationTestNoUpdate/groups") {
                            addHeader(HttpHeaders.Accept, "application/json")
                        }

                        val result: GetTopicGroupsModel = Gson().fromJson(
                            call.response.content ?: "",
                            object : TypeToken<GetTopicGroupsModel>() {}.type)

                        call.response.status() shouldBe HttpStatusCode.OK
                        result.groups.map { it.ldapResult.resultCode == ResultCode.SUCCESS } shouldEqual listOf(true, true, true)
                        result.groups.flatMap { it.members }.size shouldEqualTo 4
                    }
                }

                context("Route $APIGW") {

                    val apigwGroup = "apigw"

                    context("Get $apigwGroup group member(s)") {

                        it("Get - should return empty list group member(s) in $apigwGroup") {

                            val call = handleRequest(HttpMethod.Get, APIGW) {
                                addHeader(HttpHeaders.Accept, "application/json")
                            }

                            val status = call.response.status()
                            val result: GetApiGwGroupMembersModel = Gson().fromJson(
                                call.response.content ?: "",
                                object : TypeToken<GetApiGwGroupMembersModel>() {}.type)

                            status shouldBe HttpStatusCode.OK
                            result.members.toList() shouldBe emptyList()
                        }
                    }

                    context("Put $apigwGroup group member(s)") {

                        val newUser01 = "srvp01"
                        val newUser02 = "srvc02"
                        val userDoNotExistInLdap = "srvNonExisting"
                        val notAsystemUser = "m151888"
                        val apiGwgroupMemberToAdd = ApiGwGroupMember(newUser01, GroupMemberOperation.ADD)
                        val apiGwgroupMemberToAdd02 = ApiGwGroupMember(newUser02, GroupMemberOperation.ADD)
                        val apiGwgroupMemberToRemove = ApiGwGroupMember(newUser01, GroupMemberOperation.REMOVE)
                        val nonAdminUser = "n000002"
                        val nonAdminPwd = "itest2"
                        val admin = "n145821"
                        val adminPwd = "itest3"

                        it("Put - $apigwGroup group member(s), should return Unauthorized, User: $nonAdminUser is not an Admin") {

                            val call = handleRequest(HttpMethod.Put, APIGW) {
                                addHeader(HttpHeaders.Accept, "application/json")
                                addHeader(HttpHeaders.ContentType, "application/json")
                                addHeader(HttpHeaders.Authorization, "Basic ${encodeBase64("$nonAdminUser:$nonAdminPwd".toByteArray())}")
                                setBody(Gson().toJson(ApiGwRequest(listOf(apiGwgroupMemberToAdd))))
                            }

                            val status = call.response.status()
                            val result = call.response.content!!
                            val expectedResult = Gson().toJson(AnError("Authenticated user: $nonAdminUser is not allowed to update $apigwGroup automatically"))

                            status shouldBe HttpStatusCode.Unauthorized
                            result shouldBeEqualTo expectedResult
                        }

                        it("Put - $apigwGroup ADD group member(s), should return OK, User: $admin is Admin") {

                            val call = handleRequest(HttpMethod.Put, APIGW) {
                                addHeader(HttpHeaders.Accept, "application/json")
                                addHeader(HttpHeaders.ContentType, "application/json")
                                addHeader(HttpHeaders.Authorization, "Basic ${encodeBase64("$admin:$adminPwd".toByteArray())}")
                                setBody(Gson().toJson(ApiGwRequest(listOf(ApiGwGroupMember(newUser01, GroupMemberOperation.ADD)))))
                            }

                            val status = call.response.status()
                            val result: PutApiGwResultModel = Gson().fromJson(
                                call.response.content ?: "",
                                object : TypeToken<PutApiGwResultModel>() {}.type)

                            val expectedResult = Gson().toJson(PutApiGwResultModel(apigwGroup, ApiGwRequest(listOf(apiGwgroupMemberToAdd))))

                            status shouldBe HttpStatusCode.OK
                            Gson().toJson(result) shouldBeEqualTo expectedResult
                        }

                        it("Put - $apigwGroup group member(s), should return OK, User is $admin but $newUser01 is already in group") {

                            val call = handleRequest(HttpMethod.Put, APIGW) {
                                addHeader(HttpHeaders.Accept, "application/json")
                                addHeader(HttpHeaders.ContentType, "application/json")
                                addHeader(HttpHeaders.Authorization, "Basic ${encodeBase64("$admin:$adminPwd".toByteArray())}")
                                setBody(Gson().toJson(ApiGwRequest(listOf(ApiGwGroupMember(newUser01, GroupMemberOperation.ADD)))))
                            }

                            val status = call.response.status()
                            val result: PutApiGwResultModel = Gson().fromJson(
                                call.response.content ?: "",
                                object : TypeToken<PutApiGwResultModel>() {}.type)

                            val expectedResult = Gson().toJson(PutApiGwResultModel(apigwGroup, ApiGwRequest(listOf(apiGwgroupMemberToAdd))))

                            status shouldBe HttpStatusCode.OK
                            Gson().toJson(result) shouldBeEqualTo expectedResult
                        }

                        it("Put - $apigwGroup group member(s), should return BadRequest, User is $admin but $userDoNotExistInLdap is not in ldap") {

                            val call = handleRequest(HttpMethod.Put, APIGW) {
                                addHeader(HttpHeaders.Accept, "application/json")
                                addHeader(HttpHeaders.ContentType, "application/json")
                                addHeader(HttpHeaders.Authorization, "Basic ${encodeBase64("$admin:$adminPwd".toByteArray())}")
                                setBody(Gson().toJson(ApiGwRequest(listOf(ApiGwGroupMember(userDoNotExistInLdap, GroupMemberOperation.ADD)))))
                            }

                            val status = call.response.status()
                            val result: AnError = Gson().fromJson(
                                call.response.content ?: "",
                                object : TypeToken<AnError>() {}.type)

                            val expectedResult = Gson().toJson(AnError("Tried to add the user: $userDoNotExistInLdap. Who does not exist in current AD environment"))

                            status shouldBe HttpStatusCode.BadRequest
                            Gson().toJson(result) shouldBeEqualTo expectedResult
                        }

                        it("Put - $apigwGroup group member(s), should return BadRequest, User is $admin but $userDoNotExistInLdap is not in ldap") {

                            val call = handleRequest(HttpMethod.Put, APIGW) {
                                addHeader(HttpHeaders.Accept, "application/json")
                                addHeader(HttpHeaders.ContentType, "application/json")
                                addHeader(HttpHeaders.Authorization, "Basic ${encodeBase64("$admin:$adminPwd".toByteArray())}")
                                setBody(Gson().toJson(ApiGwRequest(listOf(ApiGwGroupMember(userDoNotExistInLdap, GroupMemberOperation.REMOVE)))))
                            }

                            val status = call.response.status()
                            val expectedResult = Gson().toJson(PutApiGwResultModel(apigwGroup, ApiGwRequest(listOf(ApiGwGroupMember(userDoNotExistInLdap, GroupMemberOperation.REMOVE)))))
                            val result: PutApiGwResultModel = Gson().fromJson(
                                call.response.content ?: "",
                                object : TypeToken<PutApiGwResultModel>() {}.type)

                            status shouldBe HttpStatusCode.OK
                            Gson().toJson(result) shouldBeEqualTo expectedResult
                        }

                        it("Put - $apigwGroup group member(s), should return BadRequest, User is $admin but $notAsystemUser is not system user") {

                            val call = handleRequest(HttpMethod.Put, APIGW) {
                                addHeader(HttpHeaders.Accept, "application/json")
                                addHeader(HttpHeaders.ContentType, "application/json")
                                addHeader(HttpHeaders.Authorization, "Basic ${encodeBase64("$admin:$adminPwd".toByteArray())}")
                                setBody(Gson().toJson(ApiGwRequest(listOf(ApiGwGroupMember(notAsystemUser, GroupMemberOperation.ADD)))))
                            }

                            val status = call.response.status()
                            val result: AnError = Gson().fromJson(
                                call.response.content ?: "",
                                object : TypeToken<AnError>() {}.type)

                            val expectedResult = Gson().toJson(AnError("Tried to add the user: $notAsystemUser. Who is not an system user"))

                            status shouldBe HttpStatusCode.BadRequest
                            Gson().toJson(result) shouldBeEqualTo expectedResult
                        }

                        it("Put - $apigwGroup REMOVE group member(s), should return OK, User: $admin is Admin") {

                            val call = handleRequest(HttpMethod.Put, APIGW) {
                                addHeader(HttpHeaders.Accept, "application/json")
                                addHeader(HttpHeaders.ContentType, "application/json")
                                addHeader(HttpHeaders.Authorization, "Basic ${encodeBase64("$admin:$adminPwd".toByteArray())}")
                                setBody(Gson().toJson(ApiGwRequest(listOf(ApiGwGroupMember(newUser01, GroupMemberOperation.REMOVE)))))
                            }

                            val status = call.response.status()
                            val result: PutApiGwResultModel = Gson().fromJson(
                                call.response.content ?: "",
                                object : TypeToken<PutApiGwResultModel>() {}.type)

                            val expectedResult = Gson().toJson(PutApiGwResultModel(apigwGroup, ApiGwRequest(listOf(apiGwgroupMemberToRemove))))

                            status shouldBe HttpStatusCode.OK
                            Gson().toJson(result) shouldBeEqualTo expectedResult
                        }

                        it("Put - $apigwGroup ADD group member(s), should return OK, User: $admin add 2 users") {

                            val call = handleRequest(HttpMethod.Put, APIGW) {
                                addHeader(HttpHeaders.Accept, "application/json")
                                addHeader(HttpHeaders.ContentType, "application/json")
                                addHeader(HttpHeaders.Authorization, "Basic ${encodeBase64("$admin:$adminPwd".toByteArray())}")
                                setBody(Gson().toJson(ApiGwRequest(listOf(apiGwgroupMemberToAdd, apiGwgroupMemberToAdd02))))
                            }

                            val status = call.response.status()
                            val result: PutApiGwResultModel = Gson().fromJson(
                                call.response.content ?: "",
                                object : TypeToken<PutApiGwResultModel>() {}.type)

                            val expectedResult = Gson().toJson(PutApiGwResultModel(apigwGroup, ApiGwRequest(listOf(apiGwgroupMemberToAdd, apiGwgroupMemberToAdd02))))

                            status shouldBe HttpStatusCode.OK
                            Gson().toJson(result) shouldBeEqualTo expectedResult
                        }

                        it("Get - should return group member(s) in $apigwGroup") {

                            val call = handleRequest(HttpMethod.Get, APIGW) {
                                addHeader(HttpHeaders.Accept, "application/json")
                            }

                            val status = call.response.status()
                            val result: GetApiGwGroupMembersModel = Gson().fromJson(
                                call.response.content ?: "",
                                object : TypeToken<GetApiGwGroupMembersModel>() {}.type)

                            val expectedResult = Gson().toJson(GetApiGwGroupMembersModel(apigwGroup, listOf(newUser01, newUser02)))

                            status shouldBe HttpStatusCode.OK
                            Gson().toJson(result) shouldBeEqualTo expectedResult
                        }

                        it("Put - $apigwGroup ADD group member(s), should return OK, User: $admin is Admin, Add one and remove one") {

                            val call = handleRequest(HttpMethod.Put, APIGW) {
                                addHeader(HttpHeaders.Accept, "application/json")
                                addHeader(HttpHeaders.ContentType, "application/json")
                                addHeader(HttpHeaders.Authorization, "Basic ${encodeBase64("$admin:$adminPwd".toByteArray())}")
                                setBody(Gson().toJson(ApiGwRequest(listOf(apiGwgroupMemberToRemove, apiGwgroupMemberToAdd02))))
                            }

                            val status = call.response.status()
                            val result: PutApiGwResultModel = Gson().fromJson(
                                call.response.content ?: "",
                                object : TypeToken<PutApiGwResultModel>() {}.type)

                            val expectedResult = Gson().toJson(PutApiGwResultModel(apigwGroup, ApiGwRequest(listOf(apiGwgroupMemberToRemove, apiGwgroupMemberToAdd02))))

                            status shouldBe HttpStatusCode.OK
                            Gson().toJson(result) shouldBeEqualTo expectedResult
                        }
                    }
                }

                context("Route GROUP IN GROUP Operations") {

                    val tp01 = "KM-tpc-01"
                    val groupInGroupTopic = "groupInGroup"

                    it("should report groups, members and GROUP in GROUP MEMBERS for topic $tp01") {

                        val call = handleRequest(HttpMethod.Get, "$GROUPS/$tp01") {
                            addHeader(HttpHeaders.Accept, "application/json")
                        }

                        val result: GetGroupMembersModel = Gson().fromJson(
                            call.response.content ?: "",
                            object : TypeToken<GetGroupMembersModel>() {}.type
                        )

                        call.response.status() shouldBe HttpStatusCode.OK
                        result.members shouldContainAll listOf(
                            "uid=n000002,ou=Users,ou=NAV,ou=BusinessUnits,dc=test,dc=local",
                            "cn=Group_00020ec3-6592-4415-a563-1ed6768d6086,OU=O365Groups,OU=Groups,OU=NAV,OU=BusinessUnits,DC=test,DC=local"
                        )
                    }

                    it("should NOT add a new MANAGER c0000-GA-BASTA_SUPERUSER to topic tpc-01, with GROUP in GROUP MEMBER") {

                        val call = handleRequest(HttpMethod.Put, "$TOPICS/tpc-01/groups") {
                            addHeader(HttpHeaders.Accept, "application/json")
                            addHeader(HttpHeaders.ContentType, "application/json")
                            // relevant user is in the right place in UserAndGroups.ldif
                            addHeader(
                                HttpHeaders.Authorization,
                                "Basic ${encodeBase64("n000003:itest4".toByteArray())}"
                            )

                            val jsonPayload = Gson().toJson(
                                UpdateKafkaGroupMember(
                                    KafkaGroupType.MANAGER,
                                    GroupMemberOperation.ADD,
                                    "0000-GA-BASTA_SUPERUSER"
                                )
                            )
                            setBody(jsonPayload)
                        }

                        call.response.status() shouldBe HttpStatusCode.ServiceUnavailable
                    }

                    it("should report groups and 2 members for topic tpc-01") {

                        val call = handleRequest(HttpMethod.Get, "$TOPICS/tpc-01/groups") {
                            addHeader(HttpHeaders.Accept, "application/json")
                        }

                        val result: GetTopicGroupsModel = Gson().fromJson(
                            call.response.content ?: "",
                            object : TypeToken<GetTopicGroupsModel>() {}.type
                        )

                        call.response.status() shouldBe HttpStatusCode.OK
                        result.groups.map { it.ldapResult.resultCode == ResultCode.SUCCESS } shouldEqual listOf(
                            true,
                            true,
                            true
                        )
                        result.groups.flatMap { it.members }.size shouldEqualTo 2
                        result.groups.flatMap { it.members } shouldContainAll listOf(
                            "uid=n000002,ou=Users,ou=NAV,ou=BusinessUnits,dc=test,dc=local",
                            "cn=Group_00020ec3-6592-4415-a563-1ed6768d6086,OU=O365Groups,OU=Groups,OU=NAV,OU=BusinessUnits,DC=test,DC=local"
                        )
                    }

                    it("should update 'delete.retention.ms' configuration for tpc-01, with GROUP in GROUP MEMBER") {

                        val call = handleRequest(HttpMethod.Put, "$TOPICS/tpc-01") {
                            addHeader(HttpHeaders.Accept, "application/json")
                            addHeader(HttpHeaders.ContentType, "application/json")
                            // relevant user is in the right place in UserAndGroups.ldif
                            addHeader(
                                HttpHeaders.Authorization,
                                "Basic ${encodeBase64("n000003:itest4".toByteArray())}"
                            )

                            val jsonPayload = Gson().toJson(
                                PutTopicConfigEntryBody(AllowedConfigEntries.DELETE_RETENTION_MS, "6600666")
                            )
                            setBody(jsonPayload)
                        }

                        call.response.status() shouldBe HttpStatusCode.OK
                    }

                    it("should add a new MANAGER 0000-GA-BASTA_SUPERUSER to topic tpc-03") {

                        val call = handleRequest(HttpMethod.Put, "$TOPICS/tpc-03/groups") {
                            addHeader(HttpHeaders.Accept, "application/json")
                            addHeader(HttpHeaders.ContentType, "application/json")
                            // relevant user is in the right place in UserAndGroups.ldif
                            addHeader(
                                HttpHeaders.Authorization,
                                "Basic ${encodeBase64("n145821:itest3".toByteArray())}"
                            )

                            val jsonPayload = Gson().toJson(
                                UpdateKafkaGroupMember(
                                    KafkaGroupType.MANAGER,
                                    GroupMemberOperation.ADD,
                                    "0000-GA-BASTA_SUPERUSER"
                                )
                            )
                            setBody(jsonPayload)
                        }

                        call.response.status() shouldBe HttpStatusCode.OK
                    }

                    it("should update 'delete.retention.ms' configuration for tpc-03, with GROUP in GROUP MEMBER") {

                        val call = handleRequest(HttpMethod.Put, "$TOPICS/tpc-03") {
                            addHeader(HttpHeaders.Accept, "application/json")
                            addHeader(HttpHeaders.ContentType, "application/json")
                            // relevant user is in the right place in UserAndGroups.ldif
                            addHeader(
                                HttpHeaders.Authorization,
                                "Basic ${encodeBase64("n000010:itest5".toByteArray())}"
                            )

                            val jsonPayload = Gson().toJson(
                                PutTopicConfigEntryBody(AllowedConfigEntries.DELETE_RETENTION_MS, "6600666")
                            )
                            setBody(jsonPayload)
                        }

                        call.response.status() shouldBe HttpStatusCode.OK
                    }

                    it("should update 'delete.retention.ms' configuration for tpc-03, with GROUP in GROUP MEMBER") {

                        val call = handleRequest(HttpMethod.Put, "$TOPICS/tpc-03") {
                            addHeader(HttpHeaders.Accept, "application/json")
                            addHeader(HttpHeaders.ContentType, "application/json")
                            // relevant user is in the right place in UserAndGroups.ldif
                            addHeader(
                                HttpHeaders.Authorization,
                                "Basic ${encodeBase64("n000011:itest6".toByteArray())}"
                            )

                            val jsonPayload = Gson().toJson(
                                PutTopicConfigEntryBody(AllowedConfigEntries.DELETE_RETENTION_MS, "6600666")
                            )
                            setBody(jsonPayload)
                        }

                        call.response.status() shouldBe HttpStatusCode.OK
                    }

                    it("should create topic $groupInGroupTopic") {

                        val call = handleRequest(HttpMethod.Post, TOPICS) {
                            addHeader(HttpHeaders.Accept, "application/json")
                            addHeader(HttpHeaders.ContentType, "application/json")
                            addHeader(HttpHeaders.Authorization, "Basic ${encodeBase64("n145821:itest3".toByteArray())}")
                            setBody(Gson().toJson(PostTopicBody(groupInGroupTopic)))
                        }

                        val result: PostTopicModel = Gson().fromJson(
                            call.response.content ?: "",
                            object : TypeToken<PostTopicModel>() {}.type)

                        call.response.status() shouldBe HttpStatusCode.OK

                        result.topicStatus shouldContain "created topic"
                        result.groupsStatus.map { it.ldapResult.resultCode.name } shouldContainAll listOf("success", "success", "success")
                        result.aclStatus shouldContain "created"
                    }

                    it("should add a new MANAGER Group_00020ec3-6592-4415-a563-1ed6768d6086 to topic $groupInGroupTopic") {

                        val call = handleRequest(HttpMethod.Put, "$TOPICS/$groupInGroupTopic/groups") {
                            addHeader(HttpHeaders.Accept, "application/json")
                            addHeader(HttpHeaders.ContentType, "application/json")
                            // relevant user is in the right place in UserAndGroups.ldif
                            addHeader(
                                HttpHeaders.Authorization,
                                "Basic ${encodeBase64("n145821:itest3".toByteArray())}"
                            )

                            val jsonPayload = Gson().toJson(
                                UpdateKafkaGroupMember(
                                    KafkaGroupType.MANAGER,
                                    GroupMemberOperation.ADD,
                                    "Group_00020ec3-6592-4415-a563-1ed6768d6086"
                                )
                            )
                            setBody(jsonPayload)
                        }

                        val result: PutTopicGMemberModel = Gson().fromJson(
                            call.response.content ?: "",
                            object : TypeToken<PutTopicGMemberModel>() {}.type)

                        call.response.status() shouldBe HttpStatusCode.OK
                        result.updaterequest.member shouldBeEqualTo "Group_00020ec3-6592-4415-a563-1ed6768d6086"
                    }

                    it("should report groups and 2 members for topic $groupInGroupTopic") {

                        val call = handleRequest(HttpMethod.Get, "$TOPICS/$groupInGroupTopic/groups") {
                            addHeader(HttpHeaders.Accept, "application/json")
                        }

                        val result: GetTopicGroupsModel = Gson().fromJson(
                            call.response.content ?: "",
                            object : TypeToken<GetTopicGroupsModel>() {}.type
                        )

                        call.response.status() shouldBe HttpStatusCode.OK
                        result.groups.map { it.ldapResult.resultCode == ResultCode.SUCCESS } shouldEqual listOf(
                            true,
                            true,
                            true
                        )
                        result.groups.flatMap { it.members }.size shouldEqualTo 2
                        result.groups.flatMap { it.members } shouldContainAll listOf(
                            "uid=n145821,ou=Users,ou=NAV,ou=BusinessUnits,dc=test,dc=local",
                            "cn=Group_00020ec3-6592-4415-a563-1ed6768d6086,OU=O365Groups,OU=Groups,OU=NAV,OU=BusinessUnits,DC=test,DC=local"
                        )
                    }

                    it("should delete topic $groupInGroupTopic for member in KM-$groupInGroupTopic") {

                        val call = handleRequest(HttpMethod.Delete, "$TOPICS/$groupInGroupTopic") {
                            addHeader(HttpHeaders.Accept, "application/json")
                            addHeader(HttpHeaders.ContentType, "application/json")
                            addHeader(HttpHeaders.Authorization, "Basic ${encodeBase64("n000003:itest4".toByteArray())}")
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
            }

            afterGroup {
                engine2.stop(1000, 2000, TimeUnit.MILLISECONDS)
                InMemoryLDAPServer.stop()
                kCluster.tearDown()
            }
        }
    }
})

private fun encodeBase64(bytes: ByteArray): String = Base64.getEncoder().encodeToString(bytes)

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import com.github.jengelman.gradle.plugins.shadow.transformers.ServiceFileTransformer
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.gradle.api.tasks.testing.logging.TestLogEvent

group = "no.nav.integrasjon"

val kotlinVersion = "1.3.61"
val kotlinLogginVersion = "1.7.8"
val ktorVersion = "1.3.1"

val jacksonDatatypeVersion = "2.10.2"

val kafkaVersion = "2.4.0"
val embeddedkafkaVersion = "2.4.0"

val prometheusVersion = "0.8.1"
val logstashEncoderVersion = "6.3"
val logbackVersion = "1.2.3"
val log4jVersion = "1.7.25"

val unboundidVersion = "4.0.14"

// do not update - breaks compatibility (should look into fixing this)
val swaggerVersion = "3.1.7"

val spekVersion = "2.0.9"
val kluentVersion = "1.60"

val konfigVersion = "1.6.10.0"

plugins {
    kotlin("jvm") version "1.3.61"
    id("org.jmailen.kotlinter") version "2.3.0"
    id("com.github.ben-manes.versions") version "0.27.0"
    id("com.github.johnrengelman.shadow") version "5.2.0"
}

repositories {
    maven(url = "http://packages.confluent.io/maven")
    maven(url = "https://dl.bintray.com/kotlin/ktor")
    mavenCentral()
    jcenter()
}

configurations.compileClasspath {
    exclude(group = "org.slf4j", module = "slf4j-log4j12")
}

dependencies {
    implementation(kotlin("stdlib"))
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8:$kotlinVersion")
    implementation("org.jetbrains.kotlin:kotlin-reflect:$kotlinVersion")

    implementation("org.apache.kafka:kafka-clients:$kafkaVersion")
    implementation("com.unboundid:unboundid-ldapsdk:$unboundidVersion")

    implementation("io.ktor:ktor-server-netty:$ktorVersion")
    implementation("io.ktor:ktor-gson:$ktorVersion")
    implementation("io.ktor:ktor-auth:$ktorVersion")
    implementation("io.ktor:ktor-locations:$ktorVersion")

    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonDatatypeVersion")

    implementation("io.github.microutils:kotlin-logging:$kotlinLogginVersion")
    implementation("ch.qos.logback:logback-classic:$logbackVersion")
    implementation("net.logstash.logback:logstash-logback-encoder:$logstashEncoderVersion")

    implementation("io.prometheus:simpleclient_common:$prometheusVersion")

    implementation("org.webjars:swagger-ui:$swaggerVersion")

    implementation("com.natpryce:konfig:$konfigVersion")

    testImplementation("org.amshove.kluent:kluent:$kluentVersion")
    testImplementation("no.nav:kafka-embedded-env:$embeddedkafkaVersion")
    testImplementation("io.ktor:ktor-server-test-host:$ktorVersion")

    testImplementation("org.spekframework.spek2:spek-dsl-jvm:$spekVersion") {
        exclude(group = "org.jetbrains.kotlin")
    }
    testRuntimeOnly("org.spekframework.spek2:spek-runner-junit5:$spekVersion") {
        exclude(group = "org.jetbrains.kotlin")
    }
}

val generatedSourcesDir = "$buildDir/generated-sources"

tasks {
    create("printVersion") {
        println(project.version)
    }
    withType <Jar>{
        manifest.attributes["Main-Class"] = "no.nav.integrasjon.MainKt"
    }
    withType<Test> {
        useJUnitPlatform {
            includeEngines("spek2")
        }
        // testLogging.showStandardStreams = true
        testLogging {
            events(TestLogEvent.PASSED, TestLogEvent.SKIPPED, TestLogEvent.FAILED)
        }
    }
    withType<Wrapper> {
        gradleVersion = "6.1"
        distributionType = Wrapper.DistributionType.BIN
    }
    withType<KotlinCompile> {
        kotlinOptions.freeCompilerArgs = listOf(
            "-Xuse-experimental=io.ktor.locations.KtorExperimentalLocationsAPI",
            "-Xuse-experimental=io.ktor.util.KtorExperimentalAPI"
        )
    }
}

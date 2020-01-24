import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import com.github.jengelman.gradle.plugins.shadow.transformers.ServiceFileTransformer
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.gradle.api.tasks.testing.logging.TestLogEvent

group = "no.nav.integrasjon"

val kotlinVersion = "1.3.61"
val kotlinLogginVersion = "1.7.8"
val ktorVersion = "1.3.0"

val jacksonDatatypeVersion = "2.10.2"

val kafkaVersion = "2.3.0"
val embeddedkafkaVersion = "2.3.0"

val prometheusVersion = "0.8.1"
val logstashEncoderVersion = "6.3"
val logbackVersion = "1.2.3"
val log4jVersion = "1.7.25"

val unboundidVersion = "4.0.14"

val swaggerVersion = "3.24.3"

val spekVersion = "2.0.9"
val kluentVersion = "1.59"

val konfigVersion = "1.6.10.0"

plugins {
    java
    kotlin("jvm") version "1.3.61"
    id("org.jmailen.kotlinter") version "2.2.0"
    id("maven-publish")
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
    compile(kotlin("stdlib"))
    compile("org.jetbrains.kotlin:kotlin-stdlib-jdk8:$kotlinVersion")
    compile("org.jetbrains.kotlin:kotlin-reflect:$kotlinVersion")

    compile("org.apache.kafka:kafka-clients:$kafkaVersion")
    compile("com.unboundid:unboundid-ldapsdk:$unboundidVersion")

    compile("io.ktor:ktor-server-netty:$ktorVersion")
    compile("io.ktor:ktor-gson:$ktorVersion")
    compile("io.ktor:ktor-auth:$ktorVersion")
    compile("io.ktor:ktor-locations:$ktorVersion")

    compile("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonDatatypeVersion")

    compile("io.github.microutils:kotlin-logging:$kotlinLogginVersion")
    compile("ch.qos.logback:logback-classic:$logbackVersion")
    compile("net.logstash.logback:logstash-logback-encoder:$logstashEncoderVersion")

    compile("io.prometheus:simpleclient_common:$prometheusVersion")

    compile("org.webjars:swagger-ui:$swaggerVersion")

    compile("com.natpryce:konfig:$konfigVersion")

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
    withType<ShadowJar> {
        transform(ServiceFileTransformer::class.java) {
            setPath("META-INF/cxf")
            include("bus-extensions.txt")
        }
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

java {
    sourceSets["main"].java.srcDirs(generatedSourcesDir)
}
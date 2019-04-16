import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import com.github.jengelman.gradle.plugins.shadow.transformers.ServiceFileTransformer
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

group = "no.nav.integrasjon"
version = "0.75-SNAPSHOT"

val kotlinVersion = "1.3.21"
val kotlinLogginVersion = "1.6.25"
val ktorVersion = "1.1.3"

// Could not find usage for this? but leave it in
// val coroutinesVersion = "0.22.5"

val jacksonDatatypeVersion = "2.9.8"

val kafkaVersion = "2.0.0"
val embeddedkafkaVersion = "2.1.1"

val prometheusVersion = "0.6.0"
val logstashEncoderVersion = "5.3"
val logbackVersion = "1.2.3"
val log4jVersion = "1.7.25"

val unboundidVersion = "4.0.10"

val swaggerVersion = "3.22.0"

val spekVersion = "2.0.2"
val kluentVersion = "1.49"

val konfigVersion = "1.6.10.0"

val mainClass = "no.nav.integrasjon.MainKt"

plugins {
    application
    java
    kotlin("jvm") version "1.3.21"
    id("org.jmailen.kotlinter") version "1.22.0"
    id("maven-publish")
    id("com.github.ben-manes.versions") version "0.21.0"
    id("com.github.johnrengelman.shadow") version "5.0.0"
}

application {
    mainClassName = mainClass
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
// configurations.compileClasspath.exclude(group = "org.slf4j", module = "slf4j-log4j12")

dependencies {
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
    withType<ShadowJar> {
        classifier = ""
        transform(ServiceFileTransformer::class.java) {
            setPath("META-INF/cxf")
            include("bus-extensions.txt")
        }
    }
    withType<Test> {
        useJUnitPlatform {
            includeEngines("spek2")
        }
        testLogging.events("passed", "skipped", "failed")
    }
    withType<Wrapper> {
        gradleVersion = "5.3"
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
package no.nav.integrasjon

import io.ktor.application.Application
//import io.ktor.server.engine.applicationEngineEnvironment
import io.ktor.server.engine.embeddedServer
//import io.ktor.server.engine.sslConnector
import io.ktor.server.netty.Netty
//import io.ktor.util.generateCertificate
//import java.io.File

fun main(args: Array<String>) {

    // see https://ktor.io/index.html for ktor enlightenment

/*    val keyStoreFile = File("build/temp.jks")
    val keyStore = generateCertificate(keyStoreFile)

    val env = applicationEngineEnvironment {

        module {
            main()
        }

        sslConnector(keyStore, "mykey", { "changeit".toCharArray() }, { "changeit".toCharArray() }) {
            this.port = 8443
            this.keyStorePath = keyStoreFile.absoluteFile
        }

    }
    embeddedServer(Netty, environment = env).start(wait = true)*/

    // start embedded netty, then fire opp ktor module and wait for connections
    embeddedServer(Netty, 8080, module = Application::main).start(wait = true)

}
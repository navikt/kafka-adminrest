package no.nav.integrasjon

import io.ktor.application.Application
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty

fun main(args: Array<String>) {

    // see https://ktor.io/index.html for ktor enlightenment

    // start embedded netty, then fire opp ktor module and wait for connections
    embeddedServer(Netty, 8080, module = Application::main).start(wait = true)
}
package no.nav.integrasjon.api.nielsfalk.ktor.swagger

import io.ktor.application.ApplicationCall
import io.ktor.http.content.URIFileContent
import io.ktor.response.respond

/**
 * @author Niels Falk, changed by Torstein Nesby
 */
class SwaggerUi {

    private val notFound = mutableListOf<String>()
    private val content = mutableMapOf<String, URIFileContent>()

    suspend fun serve(filename: String?, call: ApplicationCall) {
        when (filename) {
            in notFound -> return
            null -> return
            else -> {
                val resource = this::class.java.getResource("/META-INF/resources/webjars/swagger-ui/3.1.7/$filename")
                if (resource == null) {
                    notFound.add(filename)
                    return
                }
                call.respond(content.getOrPut(filename) { URIFileContent(resource) })
            }
        }
    }
}

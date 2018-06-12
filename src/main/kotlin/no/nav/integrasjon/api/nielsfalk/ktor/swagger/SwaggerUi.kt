package no.nav.integrasjon.api.nielsfalk.ktor.swagger
/**
import io.ktor.application.ApplicationCall
import io.ktor.content.URIFileContent
import io.ktor.response.respond

/**
 * @author Niels Falk
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

/*
private val contentTypes = mapOf(
        "html" to Html,
        "css" to CSS,
        "js" to JavaScript,
        "json" to ContentType.Application.Json.withCharset(Charsets.UTF_8),
        "png" to PNG)

private class ResourceContent(val resource: URL) : URIFileContent(resource) {
    private val bytes by lazy { resource.readBytes() }

    override val headers by lazy {
        Headers.build {
            val extension = resource.file.substring(resource.file.lastIndexOf('.') + 1)
            contentType(contentTypes[extension] ?: Html)
            contentLength(bytes.size.toLong())
        }
    }

    //override fun bytes(): ByteArray = bytes
    override fun toString() = "ResourceContent \"$resource\""
}
*/
*/
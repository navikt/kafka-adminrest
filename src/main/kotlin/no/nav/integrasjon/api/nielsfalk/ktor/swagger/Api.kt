package no.nav.integrasjon.api.nielsfalk.ktor.swagger

/**
import io.ktor.application.ApplicationCall
import io.ktor.application.call
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.http.HttpStatusCode.Companion.NotFound
import io.ktor.http.HttpStatusCode.Companion.OK
import io.ktor.locations.handle
import io.ktor.locations.location
import io.ktor.locations.post
import io.ktor.locations.put
import io.ktor.pipeline.PipelineContext
import io.ktor.request.receive
import io.ktor.routing.Route
import io.ktor.routing.method
import kotlin.reflect.KClass

/**
 * @author Niels Falk
 */

data class Metadata(val responses: Map<HttpStatusCode, KClass<*>>, val summary: String? = null) {
    var headers: KClass<*>? = null
    var parameter: KClass<*>? = null
    inline fun <reified T> header(): Metadata {
        this.headers = T::class
        return this
    }

    inline fun <reified T> parameter(): Metadata {
        this.parameter = T::class
        return this
    }
}

fun String.responds(vararg pairs: Pair<HttpStatusCode, KClass<*>>): Metadata = Metadata(responses = mapOf(*pairs), summary = this)

fun responds(pair: Pair<HttpStatusCode, KClass<*>>) = Metadata(responses = mapOf(pair))
fun responses(vararg pairs: Pair<HttpStatusCode, KClass<*>>) = Metadata(responses = mapOf(*pairs))

inline fun <reified T> ok(): Pair<HttpStatusCode, KClass<*>> = OK to T::class
inline fun notFound(): Pair<HttpStatusCode, KClass<*>> = NotFound to Unit::class

inline fun <reified LOCATION : Any, reified ENTITY : Any> Route.post(metadata: Metadata, noinline body: suspend PipelineContext<Unit, ApplicationCall>.(LOCATION, ENTITY) -> Unit): Route {
    metadata.apply<LOCATION, ENTITY>(HttpMethod.Post)
    return post<LOCATION> {
        body(this, it, call.receive())
    }
}

inline fun <reified LOCATION : Any, reified ENTITY : Any> Route.put(metadata: Metadata, noinline body: suspend PipelineContext<Unit, ApplicationCall>.(LOCATION, ENTITY) -> Unit): Route {
    metadata.apply<LOCATION, ENTITY>(HttpMethod.Put)
    return put<LOCATION> {
        body(this, it, call.receive())
    }
}

inline fun <reified LOCATION : Any> Route.get(metadata: Metadata, noinline body: suspend PipelineContext<Unit, ApplicationCall>.(LOCATION) -> Unit): Route {
    metadata.apply<LOCATION, Unit>(HttpMethod.Get)
    return location(LOCATION::class) {
        method(HttpMethod.Get) {
            handle(body)
        }
    }
}

inline fun <reified LOCATION : Any> Route.delete(metadata: Metadata, noinline body: suspend PipelineContext<Unit, ApplicationCall>.(LOCATION) -> Unit): Route {
    metadata.apply<LOCATION, Unit>(HttpMethod.Delete)
    return location(LOCATION::class) {
        method(HttpMethod.Delete) {
            handle(body)
        }
    }
}
        */
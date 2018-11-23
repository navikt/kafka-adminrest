package no.nav.integrasjon.api.nielsfalk.ktor.swagger

import io.ktor.application.ApplicationCall
import io.ktor.application.call
import io.ktor.auth.authenticate
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.http.HttpStatusCode.Companion.BadRequest
import io.ktor.http.HttpStatusCode.Companion.InternalServerError
import io.ktor.http.HttpStatusCode.Companion.OK
import io.ktor.http.HttpStatusCode.Companion.ServiceUnavailable
import io.ktor.http.HttpStatusCode.Companion.Unauthorized
import io.ktor.locations.Location
import io.ktor.locations.delete
import io.ktor.locations.get
import io.ktor.locations.post
import io.ktor.locations.put
import io.ktor.util.pipeline.PipelineContext
import io.ktor.request.receive
import io.ktor.routing.Route
import no.nav.integrasjon.AUTHENTICATION_BASIC
import no.nav.integrasjon.swagger
import kotlin.reflect.KClass

/**
 * @author Niels Falk, changed by Torstein Nesby
 */

sealed class Security
data class NoSecurity(val secSetting: List<Map<String, List<String>>> = emptyList()) : Security()
data class BasicAuthSecurity(
    val secSetting: List<Map<String, List<String>>> = listOf(mapOf("basicAuth" to emptyList()))
) : Security()

data class Metadata(
    val responses: Map<HttpStatusCode, KClass<*>>,
    val summary: String = "",
    val security: Security = NoSecurity()
) {

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

inline fun <reified LOCATION : Any, reified ENTITY_TYPE : Any> Metadata.apply(method: HttpMethod) {
    val clazz = LOCATION::class.java
    val location = clazz.getAnnotation(Location::class.java)
    val tags = clazz.getAnnotation(Group::class.java)
    applyResponseDefinitions()
    applyOperations(location, tags, method, LOCATION::class, ENTITY_TYPE::class)
}

fun Metadata.applyResponseDefinitions() =
        responses.values.forEach { addDefinition(it) }

fun <LOCATION : Any, BODY_TYPE : Any> Metadata.applyOperations(
    location: Location,
    group: Group?,
    method: HttpMethod,
    locationType: KClass<LOCATION>,
    entityType: KClass<BODY_TYPE>
) {
    swagger.paths
            .getOrPut(location.path) { mutableMapOf() }
            .put(method.value.toLowerCase(),
                    Operation(this, location, group, locationType, entityType))
}

fun String.responds(vararg pairs: Pair<HttpStatusCode, KClass<*>>): Metadata =
        Metadata(responses = mapOf(*pairs), summary = this)

fun String.securityAndReponds(security: Security, vararg pairs: Pair<HttpStatusCode, KClass<*>>): Metadata =
        Metadata(responses = mapOf(*pairs), summary = this, security = security)

inline fun <reified T> ok(): Pair<HttpStatusCode, KClass<*>> = OK to T::class
inline fun <reified T> failed(): Pair<HttpStatusCode, KClass<*>> = InternalServerError to T::class
inline fun <reified T> serviceUnavailable(): Pair<HttpStatusCode, KClass<*>> = ServiceUnavailable to T::class
inline fun <reified T> badRequest(): Pair<HttpStatusCode, KClass<*>> = BadRequest to T::class
inline fun <reified T> unAuthorized(): Pair<HttpStatusCode, KClass<*>> = Unauthorized to T::class

inline fun <reified LOCATION : Any, reified ENTITY : Any> Route.post(
    metadata: Metadata,
    noinline body: suspend PipelineContext<Unit, ApplicationCall>.(LOCATION, ENTITY) -> Unit
): Route {

    log.info { "Generating swagger spec for POST ${LOCATION::class.java.getAnnotation(Location::class.java)}" }
    metadata.apply<LOCATION, ENTITY>(HttpMethod.Post)

    return when (metadata.security) {
        is NoSecurity -> post<LOCATION> { body(this, it, call.receive()) }
        is BasicAuthSecurity -> authenticate(AUTHENTICATION_BASIC) { post<LOCATION> { body(this, it, call.receive()) } }
    }
}

inline fun <reified LOCATION : Any, reified ENTITY : Any> Route.put(
    metadata: Metadata,
    noinline body: suspend PipelineContext<Unit, ApplicationCall>.(LOCATION, ENTITY) -> Unit
): Route {

    log.info { "Generating swagger spec for PUT ${LOCATION::class.java.getAnnotation(Location::class.java)}" }
    metadata.apply<LOCATION, ENTITY>(HttpMethod.Put)

    return when (metadata.security) {
        is NoSecurity -> put<LOCATION> { body(this, it, call.receive()) }
        is BasicAuthSecurity -> authenticate(AUTHENTICATION_BASIC) { put<LOCATION> { body(this, it, call.receive()) } }
    }
}

inline fun <reified LOCATION : Any> Route.get(
    metadata: Metadata,
    noinline body: suspend PipelineContext<Unit, ApplicationCall>.(LOCATION) -> Unit
): Route {

    log.info { "Generating swagger spec for GET ${LOCATION::class.java.getAnnotation(Location::class.java)}" }
    metadata.apply<LOCATION, Unit>(HttpMethod.Get)

    return when (metadata.security) {
        is NoSecurity -> get<LOCATION> { body(this, it) }
        is BasicAuthSecurity -> authenticate(AUTHENTICATION_BASIC) { get<LOCATION> { body(this, it) } }
    }
}

inline fun <reified LOCATION : Any> Route.delete(
    metadata: Metadata,
    noinline body: suspend PipelineContext<Unit, ApplicationCall>.(LOCATION) -> Unit
): Route {

    log.info { "Generating swagger spec for DELETE ${LOCATION::class.java.getAnnotation(Location::class.java)}" }
    metadata.apply<LOCATION, Unit>(HttpMethod.Delete)

    return when (metadata.security) {
        is NoSecurity -> delete<LOCATION> { body(this, it) }
        is BasicAuthSecurity -> authenticate(AUTHENTICATION_BASIC) { delete<LOCATION> { body(this, it) } }
    }
}

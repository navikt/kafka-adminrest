@file:Suppress("MemberVisibilityCanPrivate", "unused")

package no.nav.integrasjon.api.nielsfalk.ktor.swagger

import io.ktor.http.HttpStatusCode
import io.ktor.locations.Location
import mu.KotlinLogging
import no.nav.integrasjon.swagger
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.Date
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.KType
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.full.memberProperties

/**
 * @author Niels Falk, changed by Torstein Nesby
 */
val log = KotlinLogging.logger { }

typealias ModelName = String
typealias PropertyName = String
typealias Path = String
typealias Definitions = MutableMap<ModelName, ModelData>
typealias Paths = MutableMap<Path, Methods>
typealias MethodName = String
typealias HttpStatus = String
typealias Methods = MutableMap<MethodName, Operation>

data class SecurityType(val type: String)

data class Swagger(
    val swagger: String = "2.0",
    val info: Information,
    val paths: Paths = mutableMapOf(),
    val definitions: Definitions = mutableMapOf(),
    val securityDefinitions: Map<String, SecurityType> = mapOf("basicAuth" to SecurityType("basic"))
)

data class Information(
    val description: String,
    val version: String,
    val title: String,
    val contact: Contact
)

data class Contact(
    val name: String,
    val url: String,
    val email: String
)

data class Tag(
    val name: String
)

class Operation(
    metadata: Metadata,
    location: Location,
    group: Group?,
    locationType: KClass<*>,
    entityType: KClass<*>
) {
    val tags = group?.toList()
    val summary = metadata.summary
    val parameters = mutableListOf<Parameter>().apply {
        if (entityType != Unit::class) {
            addDefinition(entityType)
            add(entityType.bodyParameter())
        }
        addAll(locationType.memberProperties.map { it.toParameter(location.path) })
        metadata.parameter?.let {
            addAll(it.memberProperties.map { it.toParameter(location.path, ParameterInputType.query) })
        }
        metadata.headers?.let {
            addAll(it.memberProperties.map { it.toParameter(location.path, ParameterInputType.header) })
        }
    }

    val responses: Map<HttpStatus, Response> = metadata.responses.map {
        val (status, kClass) = it
        addDefinition(kClass)
        status.value.toString() to Response(status, kClass)
    }.toMap()

    val security = when (metadata.security) {
        is NoSecurity -> metadata.security.secSetting
        is BasicAuthSecurity -> metadata.security.secSetting
    }
}

private fun Group.toList(): List<Tag> {
    return listOf(Tag(name))
}

fun <T, R> KProperty1<T, R>.toParameter(
    path: String,
    inputType: ParameterInputType =
            if (path.contains("{$name}"))
                ParameterInputType.path
            else
                ParameterInputType.query
): Parameter {
    return Parameter(
            toModelProperty(),
            name,
            inputType,
            required = !returnType.isMarkedNullable)
}

private fun KClass<*>.bodyParameter() =
        Parameter(referenceProperty(),
                name = "body",
                description = modelName(),
                `in` = ParameterInputType.body
        )

class Response(httpStatusCode: HttpStatusCode, kClass: KClass<*>) {
    val description = if (kClass == Unit::class) httpStatusCode.description else kClass.responseDescription()
    val schema = if (kClass == Unit::class) null else ModelReference("#/definitions/" + kClass.modelName())
}

fun KClass<*>.responseDescription(): String = modelName()

class ModelReference(val `$ref`: String)

class Parameter(
    property: Property,
    val name: String,
    val `in`: ParameterInputType,
    val description: String = property.description,
    val required: Boolean = true,
    val type: String? = property.type,
    val format: String? = property.format,
    val enum: List<String>? = property.enum,
    val items: Property? = property.items,
    val schema: ModelReference? = property.`$ref`.let { ModelReference(it) }
)

enum class ParameterInputType {
    query, path, body, header
}

class ModelData(kClass: KClass<*>) {
    val properties: Map<PropertyName, Property> =
            kClass.memberProperties
                    .map { it.name to it.toModelProperty() }
                    .toMap()
}

val propertyTypes = mapOf(
        Int::class to Property("integer", "int32"),
        Long::class to Property("integer", "int64"),
        String::class to Property("string"),
        Double::class to Property("number", "double"),
        Instant::class to Property("string", "date-time"),
        Date::class to Property("string", "date-time"),
        LocalDateTime::class to Property("string", "date-time"),
        LocalDate::class to Property("string", "date")
).mapKeys { it.key.qualifiedName }

fun <T, R> KProperty1<T, R>.toModelProperty(): Property =
        (returnType.classifier as KClass<*>)
                .toModelProperty(returnType)

private fun KClass<*>.toModelProperty(returnType: KType? = null): Property =
        propertyTypes[qualifiedName?.removeSuffix("?")]
                ?: if (returnType != null && (isSubclassOf(Collection::class) || this.isSubclassOf(Set::class))) {
                    val kClass: KClass<*> = returnType.arguments.first().type?.classifier as KClass<*>
                    Property(items = kClass.toModelProperty(), type = "array")
                } else if (returnType != null && this.isSubclassOf(Map::class)) {
                    Property(type = "object")
                } else if (returnType != null && this.isSubclassOf(String::class)) {
                    Property(type = "string")
                } else if (java.isEnum) {
                    val enumConstants = (this).java.enumConstants
                    Property(enum = enumConstants.map { (it as Enum<*>).name }, type = "string")
                } else {
                    addDefinition(this)
                    referenceProperty()
                }

private fun KClass<*>.referenceProperty(): Property =
        Property(`$ref` = "#/definitions/" + modelName(),
                description = modelName(),
                type = null)

open class Property(
    val type: String?,
    val format: String = "",
    val enum: List<String>? = null,
    val items: Property? = null,
    val description: String = "",
    val `$ref`: String = ""
)

fun addDefinition(kClass: KClass<*>) {
    if (kClass != Unit::class) {
        swagger.definitions.computeIfAbsent(kClass.modelName()) {
            log.info { "Generating swagger spec for model $it" }
            ModelData(kClass)
        }
    }
}

private fun KClass<*>.modelName(): ModelName = simpleName ?: toString()

annotation class Group(val name: String)

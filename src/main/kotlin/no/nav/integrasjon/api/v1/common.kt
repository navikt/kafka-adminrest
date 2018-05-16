package no.nav.integrasjon.api.v1

internal const val API_PREFIX = "/api/v1"
internal const val BROKERS = "$API_PREFIX/brokers"
internal const val TOPICS = "$API_PREFIX/topics"
internal const val ACLS = "$API_PREFIX/acls"
internal const val GROUPS = "$API_PREFIX/groups"

// simple data class for exceptions
internal data class AnError(val error: String)
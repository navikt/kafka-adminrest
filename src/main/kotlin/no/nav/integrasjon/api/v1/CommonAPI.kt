package no.nav.integrasjon.api.v1

internal const val API_V1 = "/api/v1"
internal const val BROKERS = "$API_V1/brokers"
internal const val TOPICS = "$API_V1/topics"
internal const val ACLS = "$API_V1/acls"
internal const val GROUPS = "$API_V1/groups"

// simple data class for exceptions
internal data class AnError(val error: String)
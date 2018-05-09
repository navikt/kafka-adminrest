package no.nav.integrasjon

/**
 * FasitProperties is a data class hosting relevant fasit properties required by this application
 */
data class FasitProperties(
    val kafkaBrokers: String = System.getenv("KAFKA_BROKERS")?.toString() ?: "",
    val kafkaClientID: String = System.getenv("KAFKA_CLIENTID")?.toString() ?: "",
    val kafkaSecurity: String = System.getenv("KAFKA_SECURITY")?.toString()?.toUpperCase() ?: "",
    val kafkaSecProt: String = System.getenv("KAFKA_SECPROT")?.toString() ?: "",
    val kafkaSaslMec: String = System.getenv("KAFKA_SASLMEC")?.toString() ?: "",
    val kafkaUser: String = System.getenv("KAFKA_USER")?.toString() ?: "",
    val kafkaPassword: String = System.getenv("KAFKA_PASSWORD")?.toString() ?: ""
)

fun FasitProperties.isEmpty(): Boolean =
        kafkaBrokers.isEmpty() || kafkaClientID.isEmpty() || kafkaSecurity.isEmpty()

fun FasitProperties.kafkaSecurityEnabled(): Boolean = kafkaSecurity == "TRUE"

fun FasitProperties.kafkaSecurityComplete(): Boolean =
        kafkaSecProt.isNotEmpty() && kafkaSaslMec.isNotEmpty() && kafkaUser.isNotEmpty() && kafkaPassword.isNotEmpty()


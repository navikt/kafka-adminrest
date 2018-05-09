package no.nav.integrasjon

import mu.KotlinLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import java.util.*

fun main(args: Array<String>) {

    val log = KotlinLogging.logger {  }

    val fp = FasitProperties()

    if (fp.isEmpty()) {
        log.error { "Missing fasit properties - $fp" }
        return
    }

    if (fp.kafkaSecurityEnabled() && !fp.kafkaSecurityComplete()) {
        log.error { "Kafka security enable, but incomplete kafka security settings - $fp" }
        return
    }

    val props = Properties()
            .apply {
                set(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, fp.kafkaBrokers)
                set(ConsumerConfig.CLIENT_ID_CONFIG, fp.kafkaClientID)
                if (fp.kafkaSecurityEnabled()) {
                    set("security.protocol", fp.kafkaSecProt)
                    set("sasl.mechanism", fp.kafkaSaslMec)
                    set("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                            "username=\"${fp.kafkaUser}\" password=\"${fp.kafkaPassword}\";")
                }
            }

    BootStrap.start(props)
}
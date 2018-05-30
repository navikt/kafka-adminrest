# kafka-adminrest

Provides a REST interface for 
- List of brokers in kafka cluster and their configuration (Broker API)
- List of all Access Control Lists in kafka cluster (ACL API)
- List of all kafka groups and their members (Group API)
- Topic creation and deletion with automatic handling of groups and access control lists. 
Each topic supports add/remove of group members and list/update topic configuration (Topic API)

Please refer to [KafkaPlainSaslServer2AD](https://github.com/navikt/KafkaPlainSaslServer2AD) for customization of 
Kafka security using LDAP groups for authorization.

## Tools
- Kotlin
- Gradle build tool
- Spek test framework for LDAP management. Missing embedded kafka with authentication & authorization for time being

## Components

1. Ktor using Netty provides the REST interfaces
2. Kafka AdminClient API v.1.0.1 for kafka interaction
3. Unboundid LDAP API v.4.0.6 for LDAPS interaction

## Testing

Tested on confluent.io version 4.0.0 and Active Directory verion 69 on 2012 R2

See related [Wiki](https://github.com/navikt/kafka-adminrest/wiki) for quick start and code structure

## Build 

```
./gradlew clean build
./gradlew shadowJar

The result is fat jar, kafka.adminclient-<version>.jar
```
### Contact us
#### Code/project related questions can be sent to 
* Torstein Nesby, `torstein.nesby@nav.no`
* Trong Huu Nguyen, `trong.huu.nguyen@nav.no`

For internal resources, send requests/questions to slack#kafka




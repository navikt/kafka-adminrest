# kafka-adminrest

[![Build Status](https://travis-ci.org/navikt/kafka-adminrest.svg?branch=master)](https://travis-ci.org/navikt/kafka-adminrest)
[![Docker Build Status](https://img.shields.io/docker/build/navikt/kafka-adminrest.svg)](https://hub.docker.com/r/navikt/kafka-adminrest/builds/)
[![Docker Automated build](https://img.shields.io/docker/automated/navikt/kafka-adminrest.svg)](https://hub.docker.com/r/navikt/kafka-adminrest/)
![Release Kafka-adminrest](https://github.com/navikt/kafka-adminrest/workflows/Release%20Kafka-adminrest/badge.svg)  


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
- Gradle build tool with lintKotlin and formatKotlin
- Spek test framework

## Components

1. Ktor using Netty provides the REST interfaces
2. Kafka AdminClient API v.1.0.1 for kafka interaction
3. Unboundid LDAP API v.4.0.6 for LDAPS interaction
4. Swagger documentation automatically generated for each endpoint

## Testing

Tested on confluent.io version 4.0.0 and Active Directory verion 69 on 2012 R2

Automated testing of LDAP management by embedded LDAP server with custom UsersAndGroups.ldif
Automated testing of (most) Ktor routes by embedded LDAP and embedded kafka cluster 

See related [Wiki](https://github.com/navikt/kafka-adminrest/wiki) for quick start and code structure

See also swagger documentation at host:port/api/v1 or at https://kafka-adminrest.dev-fss.nais.io/api/v1/

## Build 

```
./gradlew clean build installDist
```
### Contact us
Create an issue or pull request here.

Internal resources may reach us on Slack in #kafka.


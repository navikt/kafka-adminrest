package no.nav.integrasjon

import no.nav.integrasjon.ldap.LDAPBase

// Exception starter
const val EXCEPTION = "Sorry, exception happened - "

// Connection factory for which ldap in matter
fun getAuthenticationConnectionInfo(host: String, port: Int, timeout: Int) =
    LDAPBase.Companion.ConnectionInfo(host, port, timeout)

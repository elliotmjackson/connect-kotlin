// Copyright 2022-2026 The Connect Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.connectrpc.conformance.server

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.security.KeyFactory
import java.security.KeyStore
import java.security.PrivateKey
import java.security.cert.Certificate
import java.security.cert.CertificateFactory
import java.security.cert.X509Certificate
import java.security.spec.PKCS8EncodedKeySpec
import java.util.Base64

internal const val TLS_KEY_ALIAS = "server"
internal val TLS_KEY_PASSWORD: CharArray = "changeit".toCharArray()

/**
 * Builds a Java [KeyStore] containing the server's TLS cert + private key,
 * given the PEM-encoded bytes from [com.connectrpc.conformance.v1.TLSCreds].
 *
 * Accepts both PKCS#8 ("PRIVATE KEY") and PKCS#1 ("RSA PRIVATE KEY") encodings;
 * the conformance harness emits the latter for RSA keys (Go's default).
 */
internal fun buildServerKeyStore(certPem: ByteArray, keyPem: ByteArray): KeyStore {
    val cert = parseX509Certificate(certPem)
    val key = parsePrivateKey(keyPem)
    return KeyStore.getInstance("PKCS12").apply {
        load(null, null)
        setKeyEntry(
            TLS_KEY_ALIAS,
            key,
            TLS_KEY_PASSWORD,
            arrayOf<Certificate>(cert),
        )
    }
}

private fun parseX509Certificate(pem: ByteArray): X509Certificate =
    CertificateFactory.getInstance("X.509")
        .generateCertificate(ByteArrayInputStream(pem)) as X509Certificate

private fun parsePrivateKey(pem: ByteArray): PrivateKey {
    val text = String(pem, Charsets.UTF_8)
    val (label, body) = decodePem(text)
    val pkcs8 = when (label) {
        "PRIVATE KEY" -> body
        "RSA PRIVATE KEY" -> wrapRsaPkcs1AsPkcs8(body)
        else -> throw IllegalArgumentException("unsupported PEM label: $label")
    }
    val algorithm = if (label == "RSA PRIVATE KEY") "RSA" else inferKeyAlgorithm(pkcs8)
    return KeyFactory.getInstance(algorithm).generatePrivate(PKCS8EncodedKeySpec(pkcs8))
}

private fun decodePem(text: String): Pair<String, ByteArray> {
    val begin = Regex("-----BEGIN ([A-Z0-9 ]+)-----").find(text)
        ?: throw IllegalArgumentException("no PEM header found")
    val label = begin.groupValues[1].trim()
    val end = Regex("-----END $label-----").find(text)
        ?: throw IllegalArgumentException("no matching PEM footer for $label")
    val base64 = text.substring(begin.range.last + 1, end.range.first)
        .replace("\\s".toRegex(), "")
    return label to Base64.getDecoder().decode(base64)
}

private fun inferKeyAlgorithm(pkcs8: ByteArray): String {
    // Best-effort: try RSA first, then EC, falling back to RSA if both fail.
    val factories = listOf("RSA", "EC", "DSA")
    for (algo in factories) {
        runCatching {
            KeyFactory.getInstance(algo).generatePrivate(PKCS8EncodedKeySpec(pkcs8))
            return algo
        }
    }
    return "RSA"
}

/**
 * Wraps a PKCS#1 RSAPrivateKey blob in the PKCS#8 PrivateKeyInfo structure so
 * Java's [KeyFactory] can consume it.
 *
 *   PrivateKeyInfo ::= SEQUENCE {
 *       version            INTEGER (0),
 *       privateKeyAlgorithm AlgorithmIdentifier,
 *       privateKey         OCTET STRING (containing the PKCS#1 blob)
 *   }
 */
private fun wrapRsaPkcs1AsPkcs8(pkcs1: ByteArray): ByteArray {
    // AlgorithmIdentifier { 1.2.840.113549.1.1.1 (rsaEncryption), NULL }
    // Pre-baked DER: SEQUENCE { OID, NULL }
    val algId = byteArrayOf(
        0x30, 0x0d,
        0x06, 0x09, 0x2a, 0x86.toByte(), 0x48, 0x86.toByte(),
        0xf7.toByte(), 0x0d, 0x01, 0x01, 0x01,
        0x05, 0x00,
    )
    val version = byteArrayOf(0x02, 0x01, 0x00)
    val keyOctet = derWrap(0x04, pkcs1)
    return derWrap(0x30, version + algId + keyOctet)
}

private fun derWrap(tag: Int, content: ByteArray): ByteArray {
    val out = ByteArrayOutputStream()
    out.write(tag)
    val len = content.size
    when {
        len < 0x80 -> out.write(len)
        len < 0x100 -> {
            out.write(0x81); out.write(len)
        }
        len < 0x10000 -> {
            out.write(0x82); out.write(len ushr 8); out.write(len and 0xff)
        }
        else -> {
            out.write(0x83)
            out.write(len ushr 16)
            out.write((len ushr 8) and 0xff)
            out.write(len and 0xff)
        }
    }
    out.write(content)
    return out.toByteArray()
}

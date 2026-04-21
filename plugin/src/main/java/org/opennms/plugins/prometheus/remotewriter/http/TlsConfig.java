/*
 * Copyright 2026 The OpenNMS Group, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package org.opennms.plugins.prometheus.remotewriter.http;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import okhttp3.OkHttpClient;
import org.opennms.plugins.prometheus.remotewriter.config.PrometheusRemoteWriterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configures TLS on an {@link OkHttpClient.Builder} from plugin config.
 * Three mutually exclusive modes:
 * <ol>
 *   <li>Default — JDK truststore only; no configuration applied.</li>
 *   <li>Custom CA bundle — {@code tls.ca-file} points at a PEM file; its
 *       certs become the only trust anchors.</li>
 *   <li>Insecure skip-verify — accepts any certificate with any hostname.
 *       Emits a WARN log on configure and every hour thereafter.</li>
 * </ol>
 */
public final class TlsConfig {

    private static final Logger LOG = LoggerFactory.getLogger(TlsConfig.class);
    private static final long INSECURE_WARN_INTERVAL_MS = 3_600_000L;
    private static final AtomicLong LAST_INSECURE_WARN_MS = new AtomicLong(0L);

    private TlsConfig() {}

    public static void configure(OkHttpClient.Builder builder, PrometheusRemoteWriterConfig cfg) {
        if (cfg.isTlsInsecureSkipVerify()) {
            applyInsecureSkipVerify(builder);
            return;
        }
        String caFile = cfg.getTlsCaFile();
        if (caFile != null && !caFile.isEmpty()) {
            applyCustomCa(builder, caFile);
        }
        // else: leave OkHttp defaults (JDK truststore)
    }

    // -- custom CA ----------------------------------------------------------

    private static void applyCustomCa(OkHttpClient.Builder builder, String caFile) {
        try {
            Path path = Path.of(caFile);
            if (!Files.exists(path)) {
                throw new IllegalStateException("tls.ca-file does not exist: " + caFile);
            }
            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(null, null);
            try (InputStream in = Files.newInputStream(path)) {
                CertificateFactory cf = CertificateFactory.getInstance("X.509");
                Collection<? extends Certificate> certs = cf.generateCertificates(in);
                int i = 0;
                for (Certificate c : certs) {
                    if (c instanceof X509Certificate) {
                        ks.setCertificateEntry("onms-ca-" + i++, c);
                    }
                }
                if (i == 0) {
                    throw new IllegalStateException(
                        "tls.ca-file contained no X.509 certificates: " + caFile);
                }
            }

            TrustManagerFactory tmf =
                TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ks);
            TrustManager[] tms = tmf.getTrustManagers();
            X509TrustManager x509 = findX509(tms);
            if (x509 == null) {
                throw new IllegalStateException("No X509TrustManager from custom CA");
            }

            SSLContext sslCtx = SSLContext.getInstance("TLS");
            sslCtx.init(null, new TrustManager[] { x509 }, null);
            builder.sslSocketFactory(sslCtx.getSocketFactory(), x509);
            LOG.info("TLS configured with custom CA bundle: {}", caFile);
        } catch (IOException | CertificateException | NoSuchAlgorithmException
                 | java.security.KeyStoreException | java.security.KeyManagementException e) {
            throw new IllegalStateException(
                "failed to configure TLS from tls.ca-file=" + caFile + ": " + e.getMessage(), e);
        }
    }

    // -- insecure skip-verify ----------------------------------------------

    private static void applyInsecureSkipVerify(OkHttpClient.Builder builder) {
        warnInsecureIfDue();
        try {
            X509TrustManager allTrusting = new X509TrustManager() {
                @Override public void checkClientTrusted(X509Certificate[] chain, String authType) {}
                @Override public void checkServerTrusted(X509Certificate[] chain, String authType) {}
                @Override public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
            };
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(null, new TrustManager[] { allTrusting }, null);
            builder.sslSocketFactory(ctx.getSocketFactory(), allTrusting);
            builder.hostnameVerifier((hostname, session) -> true);
        } catch (NoSuchAlgorithmException | java.security.KeyManagementException e) {
            throw new IllegalStateException("failed to install insecure TLS", e);
        }
    }

    static void warnInsecureIfDue() {
        long now = System.currentTimeMillis();
        long prev = LAST_INSECURE_WARN_MS.get();
        if (now - prev >= INSECURE_WARN_INTERVAL_MS
                && LAST_INSECURE_WARN_MS.compareAndSet(prev, now)) {
            LOG.warn("tls.insecure-skip-verify is ENABLED — TLS cert and hostname "
                   + "verification are disabled. This is unsafe in production.");
        }
    }

    private static X509TrustManager findX509(TrustManager[] tms) {
        for (TrustManager tm : tms) {
            if (tm instanceof X509TrustManager x509) return x509;
        }
        return null;
    }
}

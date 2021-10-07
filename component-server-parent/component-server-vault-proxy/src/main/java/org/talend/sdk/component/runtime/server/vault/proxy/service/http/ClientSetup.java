/**
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.talend.sdk.component.runtime.server.vault.proxy.service.http;

import static java.util.Optional.ofNullable;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Disposes;
import javax.enterprise.inject.Produces;
import javax.inject.Inject;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.ClientRequestFilter;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.HttpHeaders;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.opentracing.ClientTracingRegistrar;
import org.talend.sdk.component.runtime.server.vault.proxy.configuration.Documentation;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ApplicationScoped
public class ClientSetup {

    @Inject
    @Documentation("HTTP connection timeout to vault server.")
    @ConfigProperty(name = "talend.vault.cache.client.timeout.connect", defaultValue = "30000")
    private Long connectTimeout;

    @Inject
    @Documentation("HTTP read timeout to vault server.")
    @ConfigProperty(name = "talend.vault.cache.client.timeout.read", defaultValue = "30000")
    private Long readTimeout;

    @Inject
    @Documentation("JAX-RS fully qualified name of the provides (message body readers/writers) for vault and component-server clients.")
    @ConfigProperty(name = "talend.vault.cache.client.providers")
    private Optional<String> providers;

    @Inject
    @Documentation("Should any certificate be accepted - only for dev purposes.")
    @ConfigProperty(name = "talend.vault.cache.client.certificate.acceptAny", defaultValue = "false")
    private Boolean acceptAnyCertificate;

    @Inject
    @Documentation("Where the keystore to use to connect to vault is located.")
    @ConfigProperty(name = "talend.vault.cache.client.vault.certificate.keystore.location")
    private Optional<String> vaultKeystoreLocation;

    @Inject
    @Documentation("The keystore type for `talend.vault.cache.client.vault.certificate.keystore.location`.")
    @ConfigProperty(name = "talend.vault.cache.client.vault.certificate.keystore.type")
    private Optional<String> vaultKeystoreType;

    @Inject
    @Documentation("The keystore password for `talend.vault.cache.client.vault.certificate.keystore.location`.")
    @ConfigProperty(name = "talend.vault.cache.client.vault.certificate.keystore.password", defaultValue = "changeit")
    private String vaultKeystorePassword;

    @Inject
    @Documentation("Valid hostnames for the Vault certificates (see `java.net.ssl.HostnameVerifier`).")
    @ConfigProperty(name = "talend.vault.cache.client.vault.hostname.accepted",
            defaultValue = "localhost,127.0.0.1,0:0:0:0:0:0:0:1")
    private List<String> vaultHostnames;

    @Inject
    @Documentation("The truststore type for `talend.vault.cache.client.vault.certificate.keystore.location`.")
    @ConfigProperty(name = "talend.vault.cache.client.vault.certificate.truststore.type")
    private Optional<String> vaultTruststoreType;

    @Inject
    @Documentation("Where the keystore to use to connect to Component Server is located.")
    @ConfigProperty(name = "talend.vault.cache.client.server.certificate.keystore.location")
    private Optional<String> serverKeystoreLocation;

    @Inject
    @Documentation("The keystore type for `talend.vault.cache.client.server.certificate.keystore.location`.")
    @ConfigProperty(name = "talend.vault.cache.client.server.certificate.keystore.type")
    private Optional<String> serverKeystoreType;

    @Inject
    @Documentation("The keystore password for `talend.vault.cache.client.server.certificate.keystore.location`.")
    @ConfigProperty(name = "talend.vault.cache.client.server.certificate.keystore.password", defaultValue = "changeit")
    private String serverKeystorePassword;

    @Inject
    @Documentation("The truststore type for `talend.vault.cache.client.server.certificate.keystore.location`.")
    @ConfigProperty(name = "talend.vault.cache.client.server.certificate.truststore.type")
    private Optional<String> serverTruststoreType;

    @Inject
    @Documentation("Valid hostnames for the Component Server certificates (see `java.net.ssl.HostnameVerifier`).")
    @ConfigProperty(name = "talend.vault.cache.client.server.hostname.accepted",
            defaultValue = "localhost,127.0.0.1,0:0:0:0:0:0:0:1")
    private List<String> serverHostnames;

    @Inject
    @Getter
    @Documentation("The token to use to call component-server if any.")
    @ConfigProperty(name = "talend.vault.cache.client.server.authorization")
    private Optional<String> serverToken;

    @Inject
    @Documentation("Thread pool max size for Vault client.")
    @ConfigProperty(name = "talend.vault.cache.client.executor.vault.max", defaultValue = "256")
    private Integer vaultExecutorMaxSize;

    @Inject
    @Documentation("Thread pool core size for Vault client.")
    @ConfigProperty(name = "talend.vault.cache.client.executor.vault.core", defaultValue = "64")
    private Integer vaultExecutorCoreSize;

    @Inject
    @Documentation("Thread keep alive (in ms) for Vault client thread pool.")
    @ConfigProperty(name = "talend.vault.cache.client.executor.vault.keepAlive", defaultValue = "60000")
    private Integer vaultExecutorKeepAlive;

    @Inject
    @Documentation("Thread pool max size for Component Server client.")
    @ConfigProperty(name = "talend.vault.cache.client.executor.server.max", defaultValue = "256")
    private Integer serverExecutorMaxSize;

    @Inject
    @Documentation("Thread pool core size for Component Server client.")
    @ConfigProperty(name = "talend.vault.cache.client.executor.server.core", defaultValue = "64")
    private Integer serverExecutorCoreSize;

    @Inject
    @Documentation("Thread keep alive (in ms) for Component Server client thread pool.")
    @ConfigProperty(name = "talend.vault.cache.client.executor.server.keepAlive", defaultValue = "60000")
    private Integer serverExecutorKeepAlive;

    @Inject
    @Documentation("Base URL to connect to Vault.")
    @ConfigProperty(name = "talend.vault.cache.vault.url")
    private String vaultUrl;

    @Inject
    @Documentation("Base URL to connect to Component Server")
    @ConfigProperty(name = "talend.vault.cache.talendComponentKit.url")
    private String talendComponentKitUrl;

    @Produces
    @Http(Http.Type.VAULT)
    @ApplicationScoped
    public WebTarget vaultTarget(@Http(Http.Type.VAULT) final Client client) {
        return client.target(vaultUrl);
    }

    @Produces
    @Http(Http.Type.TALEND_COMPONENT_KIT)
    @ApplicationScoped
    public WebTarget talendComponentKitTarget(@Http(Http.Type.TALEND_COMPONENT_KIT) final Client client) {
        return client.target(talendComponentKitUrl);
    }

    @Produces
    @ApplicationScoped
    @Http(Http.Type.TALEND_COMPONENT_KIT)
    public ExecutorService serverExecutorService() {
        return createExecutor(serverExecutorCoreSize, serverExecutorMaxSize, serverExecutorKeepAlive, "server");
    }

    public void releaseServerExecutor(
            @Disposes @Http(Http.Type.TALEND_COMPONENT_KIT) final ExecutorService executorService) {
        // should we be less aggressive? normally traffic is already off when hitting that so "ok"
        executorService.shutdownNow();
    }

    @Produces
    @ApplicationScoped
    @Http(Http.Type.VAULT)
    public ExecutorService vaultExecutorService() {
        return createExecutor(vaultExecutorCoreSize, vaultExecutorMaxSize, vaultExecutorKeepAlive, "vault");
    }

    public void releaseVaultExecutor(@Disposes @Http(Http.Type.VAULT) final ExecutorService executorService) {
        // should we be less aggressive? normally traffic is already off when hitting that so "ok"
        executorService.shutdownNow();
    }

    @Produces
    @ApplicationScoped
    @Http(Http.Type.VAULT)
    public Client vaultClient(@Http(Http.Type.VAULT) final ExecutorService executor) {
        return createClient(executor, vaultKeystoreLocation, vaultKeystoreType, vaultKeystorePassword,
                vaultTruststoreType, vaultHostnames).build();
    }

    @Produces
    @ApplicationScoped
    @Http(Http.Type.TALEND_COMPONENT_KIT)
    public Client serverClient(@Http(Http.Type.TALEND_COMPONENT_KIT) final ExecutorService executor) {
        ClientBuilder builder = createClient(executor, serverKeystoreLocation, serverKeystoreType,
                serverKeystorePassword, serverTruststoreType, serverHostnames);
        if (serverToken.isPresent()) {
            final String token = serverToken.get();
            builder = builder
                    .register((ClientRequestFilter) requestContext -> requestContext
                            .getHeaders()
                            .putSingle(HttpHeaders.AUTHORIZATION, token));
        }
        return builder.build();
    }

    public void releaseVaultClient(@Disposes @Http(Http.Type.VAULT) final Client client) {
        client.close();
    }

    public void releaseServerClient(@Disposes @Http(Http.Type.TALEND_COMPONENT_KIT) final Client client) {
        client.close();
    }

    private ThreadPoolExecutor createExecutor(final int core, final int max, final long keepAlive,
            final String nameMarker) {
        return new ThreadPoolExecutor(core, max, keepAlive, MILLISECONDS, new LinkedBlockingQueue<>(),
                new ThreadFactory() {

                    private final ThreadGroup group = ofNullable(System.getSecurityManager())
                            .map(SecurityManager::getThreadGroup)
                            .orElseGet(() -> Thread.currentThread().getThreadGroup());

                    private final AtomicInteger threadNumber = new AtomicInteger(1);

                    @Override
                    public Thread newThread(final Runnable r) {
                        final Thread t = new Thread(group, r,
                                "talend-vault-proxy-" + nameMarker + "-" + threadNumber.getAndIncrement(), 0);
                        if (t.isDaemon()) {
                            t.setDaemon(false);
                        }
                        if (t.getPriority() != Thread.NORM_PRIORITY) {
                            t.setPriority(Thread.NORM_PRIORITY);
                        }
                        return t;
                    }
                });
    }

    private ClientBuilder createClient(final ExecutorService executor, final Optional<String> keystoreLocation,
            final Optional<String> keystoreType, final String keystorePassword, final Optional<String> truststoreType,
            final List<String> serverHostnames) {
        final ClientBuilder builder = ClientBuilder.newBuilder();
        builder.connectTimeout(connectTimeout, MILLISECONDS);
        builder.readTimeout(readTimeout, MILLISECONDS);
        builder.executorService(executor);
        if (acceptAnyCertificate) {
            builder.hostnameVerifier((host, session) -> true);
            builder.sslContext(createUnsafeSSLContext());
        } else if (keystoreLocation.isPresent()) {
            builder.hostnameVerifier((host, session) -> serverHostnames.contains(host));
            builder.sslContext(createSSLContext(keystoreLocation, keystoreType, keystorePassword, truststoreType));
        }
        providers.map(it -> Stream.of(it.split(",")).map(String::trim).filter(v -> !v.isEmpty()).map(fqn -> {
            try {
                return Thread.currentThread().getContextClassLoader().loadClass(fqn).getConstructor().newInstance();
            } catch (final Exception e) {
                log.warn("Can't add provider " + fqn + ": " + e.getMessage(), e);
                return null;
            }
        }).filter(Objects::nonNull)).ifPresent(it -> it.forEach(builder::register));
        return ClientTracingRegistrar.configure(builder);
    }

    private SSLContext createUnsafeSSLContext() {
        final TrustManager[] trustManagers = { new X509TrustManager() {

            @Override
            public void checkClientTrusted(final X509Certificate[] x509Certificates, final String s) {
                // no-op
            }

            @Override
            public void checkServerTrusted(final X509Certificate[] x509Certificates, final String s) {
                // no-op
            }

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return null;
            }
        } };
        try {
            final SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustManagers, new java.security.SecureRandom());
            return sslContext;
        } catch (final NoSuchAlgorithmException | KeyManagementException e) {
            throw new IllegalStateException(e);
        }
    }

    private SSLContext createSSLContext(final Optional<String> keystoreLocation, final Optional<String> keystoreType,
            final String keystorePassword, final Optional<String> truststoreType) {
        final File source = new File(keystoreLocation.orElseThrow(IllegalArgumentException::new));
        if (!source.exists()) {
            throw new IllegalArgumentException(source + " does not exist");
        }
        final KeyStore keyStore;
        try (final FileInputStream stream = new FileInputStream(source)) {
            keyStore = KeyStore.getInstance(keystoreType.orElseGet(KeyStore::getDefaultType));
            keyStore.load(stream, keystorePassword.toCharArray());
        } catch (final KeyStoreException | NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        } catch (final CertificateException | IOException e) {
            throw new IllegalArgumentException(e);
        }
        try {
            final TrustManagerFactory trustManagerFactory =
                    TrustManagerFactory.getInstance(truststoreType.orElseGet(TrustManagerFactory::getDefaultAlgorithm));
            trustManagerFactory.init(keyStore);
            final SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustManagerFactory.getTrustManagers(), new java.security.SecureRandom());
            return sslContext;
        } catch (final KeyStoreException | NoSuchAlgorithmException | KeyManagementException e) {
            throw new IllegalStateException(e);
        }
    }
}

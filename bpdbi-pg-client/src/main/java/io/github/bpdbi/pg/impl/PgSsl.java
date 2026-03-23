package io.github.bpdbi.pg.impl;

import io.github.bpdbi.core.ConnectionConfig;
import io.github.bpdbi.core.DbConnectionException;
import io.github.bpdbi.core.SslMode;
import io.github.bpdbi.pg.impl.codec.PgEncoder;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

/**
 * SSL/TLS upgrade logic for Postgres connections. Handles the SSLRequest protocol message and
 * SSLContext construction from various certificate sources.
 */
public final class PgSsl {

  private PgSsl() {}

  /**
   * Upgrade a plain socket to SSL/TLS using the Postgres SSLRequest protocol. Sends the 8-byte
   * SSLRequest message, reads the server's single-byte response, and wraps the socket in an
   * SSLSocket if the server accepts.
   */
  public static Socket upgradeToSsl(Socket socket, ConnectionConfig config) throws IOException {
    SslMode sslMode = config.sslMode();
    String host = config.host();
    int port = config.port() > 0 ? config.port() : 5432;

    // Send SSLRequest (not a normal startup message — no type byte)
    var encoder = new PgEncoder();
    encoder.writeSSLRequest();
    encoder.flush(socket.getOutputStream());

    // Read single byte response: 'S' = upgrade, 'N' = no SSL
    int response = socket.getInputStream().read();
    if (response == 'S') {
      try {
        SSLContext ctx = buildSslContext(config);
        SSLSocketFactory factory = ctx.getSocketFactory();
        SSLSocket sslSocket = (SSLSocket) factory.createSocket(socket, host, port, true);
        sslSocket.setUseClientMode(true);

        // Enable hostname verification for VERIFY_FULL
        if (sslMode == SslMode.VERIFY_FULL || config.hostnameVerification()) {
          SSLParameters sslParams = sslSocket.getSSLParameters();
          sslParams.setEndpointIdentificationAlgorithm("HTTPS");
          sslSocket.setSSLParameters(sslParams);
        }

        sslSocket.startHandshake();
        return sslSocket;
      } catch (Exception e) {
        throw new IOException("SSL handshake failed", e);
      }
    } else if (response == 'N') {
      if (sslMode == SslMode.REQUIRE
          || sslMode == SslMode.VERIFY_CA
          || sslMode == SslMode.VERIFY_FULL) {
        socket.close();
        throw new IOException("Server does not support SSL but sslMode=" + sslMode);
      }
      // PREFER: fall back to non-SSL
      return socket;
    } else {
      socket.close();
      throw new IOException("Unexpected SSL response: " + (char) response);
    }
  }

  /**
   * Build an SSLContext based on the connection configuration.
   *
   * <p>Priority order:
   *
   * <ol>
   *   <li>User-provided {@link ConnectionConfig#sslContext()} — used as-is
   *   <li>PEM certificate file via {@link ConnectionConfig#pemCertPath()}
   *   <li>JKS trust store via {@link ConnectionConfig#trustStorePath()}
   *   <li>For VERIFY_CA/VERIFY_FULL — system default trust store
   *   <li>For REQUIRE/PREFER — trust-all (no certificate verification)
   * </ol>
   */
  private static SSLContext buildSslContext(ConnectionConfig config) {
    // 1. User-provided SSLContext
    if (config.sslContext() != null) {
      return config.sslContext();
    }

    try {
      SSLContext ctx = SSLContext.getInstance("TLS");

      // 2. PEM certificate file
      if (config.pemCertPath() != null) {
        KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
        ks.load(null, null);
        try (InputStream certIn = new FileInputStream(config.pemCertPath())) {
          CertificateFactory cf = CertificateFactory.getInstance("X.509");
          int i = 0;
          for (var cert : cf.generateCertificates(certIn)) {
            ks.setCertificateEntry("cert-" + (i++), cert);
          }
        }
        TrustManagerFactory tmf =
            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);
        ctx.init(null, tmf.getTrustManagers(), null);
        return ctx;
      }

      // 3. JKS trust store
      if (config.trustStorePath() != null) {
        KeyStore ks = KeyStore.getInstance("JKS");
        char[] pass =
            config.trustStorePassword() != null ? config.trustStorePassword().toCharArray() : null;
        try (InputStream tsIn = new FileInputStream(config.trustStorePath())) {
          ks.load(tsIn, pass);
        }
        TrustManagerFactory tmf =
            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ks);
        ctx.init(null, tmf.getTrustManagers(), null);
        return ctx;
      }

      // 4. For VERIFY_CA/VERIFY_FULL — use system default trust store
      if (config.sslMode() == SslMode.VERIFY_CA || config.sslMode() == SslMode.VERIFY_FULL) {
        TrustManagerFactory tmf =
            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init((KeyStore) null); // uses default system trust store
        ctx.init(null, tmf.getTrustManagers(), null);
        return ctx;
      }

      // 5. REQUIRE/PREFER — trust-all (no certificate verification)
      ctx.init(
          null,
          new TrustManager[] {
            new X509TrustManager() {
              public void checkClientTrusted(X509Certificate[] certs, String authType) {}

              public void checkServerTrusted(X509Certificate[] certs, String authType) {}

              public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
              }
            }
          },
          null);
      return ctx;
    } catch (Exception e) {
      throw new DbConnectionException("Failed to create SSL context", e);
    }
  }
}

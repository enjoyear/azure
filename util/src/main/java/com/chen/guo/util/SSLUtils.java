package com.chen.guo.util;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.SecureRandom;

import org.apache.commons.io.FileUtils;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class SSLUtils {

  public static void main(String[] args) {
    new SSLUtils().getSSLSocketFactory();
  }

  public SSLSocketFactory getSSLSocketFactory() {
    SSLContext sslContext;
    try {
      sslContext = buildSSLContext();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return sslContext.getSocketFactory();
  }

  /**
   * build the ssl context for data vault enabled https request, adopted from SSLContextBuilder in Linkedin container MP.
   */
  private SSLContext buildSSLContext()
      throws Exception {
    String securityProtocol = "SSL";
    String sslProtocol = "TLS";

    String keyStoreLocation = "/Users/chguo/Downloads/identity.p12";
    String keyStorePassword = "work_around_jdk-6879539";
    String keyStoreType = "pkcs12";
    String keyManagerAlgorithm = "SunX509";

    //String trustStoreLocation = "/Library/Java/JavaVirtualMachines/jdk1.8.0_212.jdk/Contents/Home/jre/lib/security/cacerts";
    String trustStoreLocation = "/etc/riddler/cacerts";
    String trustStorePassword = "changeit";
    String trustStoreType = "JKS";
    String trustManagerAlgorithm = "SunX509";

    String keyPassword = "work_around_jdk-6879539";
    String secureRandomImplmentation = "SHA1PRNG";

    log.info("Load the key Store");
    final KeyStore keyStore = KeyStore.getInstance(keyStoreType);
    keyStore.load(toInputStream(new File(keyStoreLocation)), keyStorePassword.toCharArray());

    log.info("Load trust store");
    final KeyStore trustStore = KeyStore.getInstance(trustStoreType);
    trustStore.load(toInputStream(new File(trustStoreLocation)), trustStorePassword.toCharArray());

    log.info("Set key manager from key store");
    final KeyManagerFactory kmf = KeyManagerFactory.getInstance(keyManagerAlgorithm);
    kmf.init(keyStore, keyStorePassword.toCharArray());

    log.info("Set trust manager from trust store");
    final TrustManagerFactory tmf = TrustManagerFactory.getInstance(trustManagerAlgorithm);
    tmf.init(trustStore);

    log.info("Set context with security protocol");
    final SSLContext secureContext = SSLContext.getInstance(sslProtocol);
    secureContext
        .init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
    return secureContext;
  }

  private InputStream toInputStream(File storeFile) {
    try {
      byte[] data = FileUtils.readFileToByteArray(storeFile);
      return new ByteArrayInputStream(data);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

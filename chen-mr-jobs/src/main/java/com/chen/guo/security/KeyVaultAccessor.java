package com.chen.guo.security;

import com.microsoft.azure.keyvault.KeyVaultClient;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCSException;

import java.io.IOException;
import java.security.cert.CertificateException;

public class KeyVaultAccessor {
  public static void main(String[] args) throws OperatorCreationException, CertificateException, PKCSException, IOException {

    String keyVaultURL = "https://your_key_vault_name.vault.azure.net";
    String clientId = "";
    String clientSecret = "";

    KeyVaultClient kvClientCertAuth = KeyVaultCertificateAuthenticator.getAuthenticatedClient(System.getProperty("CERTIFICATE_PATH"), System.getProperty("CERTIFICATE_PASSWORD"));

  }
}

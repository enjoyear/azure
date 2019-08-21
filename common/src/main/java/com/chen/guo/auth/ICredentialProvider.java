package com.chen.guo.auth;

import com.microsoft.aad.adal4j.ClientCredential;

public interface ICredentialProvider {
  ClientCredential getClientCredential(String clientName);

  /**
   * Active Directory Id(tenant id)
   */
  String getADId();

  String getSubscriptionId();
}

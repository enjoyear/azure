package com.chen.guo;

public interface ICredentialProvider {
  String getClientId(String clientName);

  String getClientSecret(String clientName);

  /**
   * Active Directory Id(tenant id)
   */
  String getADId();

  String getSubscriptionId();
}

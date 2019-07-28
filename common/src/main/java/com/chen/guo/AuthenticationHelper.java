package com.chen.guo;

import com.microsoft.aad.adal4j.AuthenticationCallback;

public class AuthenticationHelper {
  public final static AuthenticationCallback authenticationCallback = new AuthenticationCallback() {
    @Override
    public void onSuccess(Object result) {
      System.out.println("Success: " + result.toString());
    }

    @Override
    public void onFailure(Throwable exc) {
      System.out.println("Failed: " + exc.toString());
    }
  };
  public final static String managementResourceUri = "https://management.core.windows.net/";

  private final ICredentialProvider _credentialProvider;

  public AuthenticationHelper(ICredentialProvider credentialProvider) {
    _credentialProvider = credentialProvider;
  }

  public String getAuthorityUri() {
    //String authorityUri = "https://login.microsoftonline.com/microsoft.com";
    //String authorityUri = "https://login.windows.net/common/oauth2/authorize";
    //https://login.microsoftonline.com/common/federation/oauth2
    return String.format("https://login.microsoftonline.com/%s", _credentialProvider.getADId());
  }


}

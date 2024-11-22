package edu.internet2.middleware.grouper.app.scim2Provisioning;


public class ScimSettings {
  
  private String orgName;
  
  private String scimNamePatchStrategy;
  
  private String acceptHeader;
  
  public String getOrgName() {
    return orgName;
  }

  
  public void setOrgName(String orgName) {
    this.orgName = orgName;
  }

  
  public String getScimNamePatchStrategy() {
    return scimNamePatchStrategy;
  }

  
  public void setScimNamePatchStrategy(String scimNamePatchStrategy) {
    this.scimNamePatchStrategy = scimNamePatchStrategy;
  }

  public String getAcceptHeader() {
    return acceptHeader;
  }

  public void setAcceptHeader(String acceptHeader) {
    this.acceptHeader = acceptHeader;
  }

}

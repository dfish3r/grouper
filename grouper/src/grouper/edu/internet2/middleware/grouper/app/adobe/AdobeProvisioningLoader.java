package edu.internet2.middleware.grouper.app.adobe;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import edu.internet2.middleware.grouper.app.provisioning.GrouperProvisioningLoader;
import edu.internet2.middleware.grouper.app.provisioning.ProvisioningEntity;
import edu.internet2.middleware.grouper.app.provisioning.ProvisioningGroup;
import edu.internet2.middleware.grouper.app.provisioning.ProvisioningMembership;
import edu.internet2.middleware.grouper.util.GrouperUtil;

public class AdobeProvisioningLoader extends GrouperProvisioningLoader {
  
  @Override
  public String getLoaderEntityTableName() {
    return "grouper_prov_adobe_user";
  }
  

  public List<String> getLoaderEntityColumnNames() {
    return GrouperUtil.toList("config_id", "user_id", "username", "email",
         "status", "adobe_type", "firstname", "lastname", "domain", "country");
  }
  
  @Override
  public List<String> getLoaderEntityKeyColumnNames() {
    return GrouperUtil.toList("config_id", "user_id");
  }
  
  @Override
  public String getLoaderGroupTableName() {
    return "grouper_prov_adobe_group";
  }
  

  public List<String> getLoaderGroupColumnNames() {
    return GrouperUtil.toList("config_id", "group_id", "name", "adobe_type",
         "product_name", "member_count", "license_quota");
  }
  
  @Override
  public List<String> getLoaderGroupKeyColumnNames() {
    return GrouperUtil.toList("config_id", "group_id");
  }
  
  @Override
  public String getLoaderMembershipTableName() {
    return "grouper_prov_adobe_membership";
  }
  

  public List<String> getLoaderMembershipColumnNames() {
    return GrouperUtil.toList("config_id", "group_id", "user_id");
  }
  
  @Override
  public List<String> getLoaderMembershipKeyColumnNames() {
    return GrouperUtil.toList("config_id", "group_id", "user_id");
  }
  
  
  @Override
  public List<Object[]> retrieveLoaderEntityTableDataFromDataBean() {
    
    List<ProvisioningEntity> targetProvisioningEntities = this.getGrouperProvisioner().retrieveGrouperProvisioningData().retrieveTargetProvisioningEntities();
    
    List<Object[]> result = new ArrayList<>();
    
    for (ProvisioningEntity targetProvisioningEntity: targetProvisioningEntities) {
      
      GrouperAdobeUser grouperAdobeUser = (GrouperAdobeUser)targetProvisioningEntity.getProvisioningEntityWrapper().getTargetNativeEntity();
      
      Object[] row = new Object[this.getLoaderEntityColumnNames().size()];
      
      row[0] = this.getGrouperProvisioner().getConfigId();
      row[1] = grouperAdobeUser.getId();
      row[2] = grouperAdobeUser.getUserName();
      row[3] = grouperAdobeUser.getEmail();
      row[4] = grouperAdobeUser.getStatus();
      row[5] = grouperAdobeUser.getType();
      row[6] = grouperAdobeUser.getFirstName();
      row[7] = grouperAdobeUser.getLastName();
      row[8] = grouperAdobeUser.getDomain();
      row[9] = grouperAdobeUser.getCountry();
      
      result.add(row);
      
    }
    
    return result;
  }
  
  @Override
  public List<Object[]> retrieveLoaderGroupTableDataFromDataBean() {
    
    List<ProvisioningGroup> targetProvisioningGroups = this.getGrouperProvisioner().retrieveGrouperProvisioningData().retrieveTargetProvisioningGroups();
    
    List<Object[]> result = new ArrayList<>();
    
    for (ProvisioningGroup targetProvisioningGroup: targetProvisioningGroups) {
      
      GrouperAdobeGroup grouperAdobeGroup = (GrouperAdobeGroup)targetProvisioningGroup.getProvisioningGroupWrapper().getTargetNativeGroup();
      
      Object[] row = new Object[this.getLoaderGroupColumnNames().size()];
      
      row[0] = this.getGrouperProvisioner().getConfigId();
      row[1] = grouperAdobeGroup.getId();
      row[2] = grouperAdobeGroup.getName();
      row[3] = grouperAdobeGroup.getType();
      row[4] = grouperAdobeGroup.getProductName();
      row[5] = grouperAdobeGroup.getMemberCount();
      row[6] = grouperAdobeGroup.getLicenseQuota();
      
      result.add(row);
      
    }
    
    return result;
  }
  
  @Override
  public List<Object[]> retrieveLoaderMembershipTableDataFromDataBean() {
    
    List<ProvisioningMembership> targetProvisioningMemberships = this.getGrouperProvisioner().retrieveGrouperProvisioningData().retrieveTargetProvisioningMemberships();
    
    List<Object[]> result = new ArrayList<>();
    
    for (ProvisioningMembership targetProvisioningMembership: targetProvisioningMemberships) {
      
//      GrouperAdobeMembership grouperAdobeMembership = (GrouperAdobeMembership)targetProvisioningMembership.getProvisioningMembershipWrapper().getTargetNativeMembership();
      
      ProvisioningEntity provisioningEntity = targetProvisioningMembership.getProvisioningEntity();
      ProvisioningGroup provisioningGroup = targetProvisioningMembership.getProvisioningGroup();
      
      String userId = null;
      Long groupId = null;
      
      if (provisioningEntity != null) {
        userId = provisioningEntity.retrieveAttributeValueString("id"); 
      }
      if (provisioningGroup != null) {
        groupId = provisioningGroup.retrieveAttributeValueLong("id"); 
      }
      
      if (StringUtils.isBlank(userId) || groupId == null) {
        continue;
      }
      
      Object[] row = new Object[this.getLoaderMembershipColumnNames().size()];
      row[0] = this.getGrouperProvisioner().getConfigId();
      row[1] = groupId;
      row[2] = userId;
      result.add(row);
    }
    
    return result;
  }

}

package edu.internet2.middleware.grouper.app.remedyV2.digitalMarketplace;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.internet2.middleware.grouper.app.provisioning.GrouperProvisioningLists;
import edu.internet2.middleware.grouper.app.provisioning.ProvisioningEntity;
import edu.internet2.middleware.grouper.app.provisioning.ProvisioningGroup;
import edu.internet2.middleware.grouper.app.provisioning.ProvisioningMembership;
import edu.internet2.middleware.grouper.app.provisioning.ProvisioningObjectChange;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.GrouperProvisionerDaoCapabilities;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.GrouperProvisionerTargetDaoBase;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.TargetDaoDeleteGroupRequest;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.TargetDaoDeleteGroupResponse;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.TargetDaoDeleteMembershipRequest;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.TargetDaoDeleteMembershipResponse;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.TargetDaoInsertGroupRequest;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.TargetDaoInsertGroupResponse;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.TargetDaoInsertMembershipRequest;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.TargetDaoInsertMembershipResponse;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.TargetDaoRetrieveAllDataRequest;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.TargetDaoRetrieveAllDataResponse;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.TargetDaoRetrieveAllEntitiesRequest;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.TargetDaoRetrieveAllEntitiesResponse;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.TargetDaoRetrieveAllGroupsRequest;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.TargetDaoRetrieveAllGroupsResponse;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.TargetDaoRetrieveAllMembershipsRequest;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.TargetDaoRetrieveAllMembershipsResponse;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.TargetDaoRetrieveEntitiesRequest;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.TargetDaoRetrieveEntitiesResponse;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.TargetDaoRetrieveEntityRequest;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.TargetDaoRetrieveEntityResponse;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.TargetDaoRetrieveGroupRequest;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.TargetDaoRetrieveGroupResponse;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.TargetDaoRetrieveGroupsRequest;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.TargetDaoRetrieveGroupsResponse;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.TargetDaoRetrieveMembershipsByEntityRequest;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.TargetDaoRetrieveMembershipsByEntityResponse;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.TargetDaoSendMembershipChangesToTargetRequest;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.TargetDaoSendMembershipChangesToTargetResponse;
import edu.internet2.middleware.grouper.app.provisioning.targetDao.TargetDaoTimingInfo;
import edu.internet2.middleware.grouper.util.GrouperHttpClient;
import edu.internet2.middleware.grouper.util.GrouperHttpClientLog;
import edu.internet2.middleware.grouper.util.GrouperUtil;
import edu.internet2.middleware.grouperClient.jdbc.tableSync.GcGrouperSyncErrorCode;
import org.apache.commons.lang3.StringUtils;

/**
 * target dao for remedy digital marketplace
 */
public class GrouperDigitalMarketplaceTargetDao extends GrouperProvisionerTargetDaoBase  {
  
  @Override
  public boolean loggingStart() {
    return GrouperHttpClient.logStart(new GrouperHttpClientLog());
  }

  @Override
  public String loggingStop() {
    return GrouperHttpClient.logEnd();
  }
  
  
  
  @Override
  public TargetDaoSendMembershipChangesToTargetResponse sendMembershipChangesToTarget(
      TargetDaoSendMembershipChangesToTargetRequest targetDaoSendMembershipChangesToTargetRequest) {
    
    long startNanos = System.nanoTime();

    GrouperUtil.assertion(GrouperUtil.length(targetDaoSendMembershipChangesToTargetRequest.getTargetMembershipReplaces()) == 0, "There should not be any replaces");
    GrouperUtil.assertion(GrouperUtil.length(targetDaoSendMembershipChangesToTargetRequest.getTargetMembershipUpdates()) == 0, "There should not be any updates");
    
    Set<String> loginNames = new HashSet<String>();
    
    Map<String, Set<String>> loginNameToGroupNamesToInsert = new HashMap<>();
    Map<String, Set<String>> loginNameToGroupNamesToDelete = new HashMap<>();
    Map<String, Set<ProvisioningMembership>> loginNameToGrouperTargetMembership = new HashMap<>();
    
    for (ProvisioningMembership grouperTargetMembership : GrouperUtil.nonNull(targetDaoSendMembershipChangesToTargetRequest.getTargetMembershipDeletes())) {
      String groupName = grouperTargetMembership.retrieveAttributeValueString("groupName");
      String loginName = grouperTargetMembership.retrieveAttributeValueString("loginName");
      
      if (StringUtils.isBlank(groupName)) {
        continue;
      }
      if (StringUtils.isBlank(loginName)) {
        continue;
      }
      
      loginNames.add(loginName);
      Set<String> groupNamesToDelete = loginNameToGroupNamesToDelete.get(loginName);
      if (groupNamesToDelete == null) {
        groupNamesToDelete = new HashSet<>();
        loginNameToGroupNamesToDelete.put(loginName, groupNamesToDelete);
      }
      
      groupNamesToDelete.add(groupName);

      Set<ProvisioningMembership> grouperTargetMemberships = loginNameToGrouperTargetMembership.get(loginName);
      if (grouperTargetMemberships == null) {
        grouperTargetMemberships = new HashSet<>();
        loginNameToGrouperTargetMembership.put(loginName, grouperTargetMemberships);
      }
      
      grouperTargetMemberships.add(grouperTargetMembership);

    }
    
    for (ProvisioningMembership grouperTargetMembership : GrouperUtil.nonNull(targetDaoSendMembershipChangesToTargetRequest.getTargetMembershipInserts())) {
      String groupName = grouperTargetMembership.retrieveAttributeValueString("groupName");
      String loginName = grouperTargetMembership.retrieveAttributeValueString("loginName");
      
      if (StringUtils.isBlank(groupName)) {
        continue;
      }
      if (StringUtils.isBlank(loginName)) {
        continue;
      }

      loginNames.add(loginName);
      Set<String> groupNamesToInsert = loginNameToGroupNamesToInsert.get(loginName);
      if (groupNamesToInsert == null) {
        groupNamesToInsert = new HashSet<>();
        loginNameToGroupNamesToInsert.put(loginName, groupNamesToInsert);
      }
      
      groupNamesToInsert.add(groupName);
      
      Set<ProvisioningMembership> grouperTargetMemberships = loginNameToGrouperTargetMembership.get(loginName);
      if (grouperTargetMemberships == null) {
        grouperTargetMemberships = new HashSet<>();
        loginNameToGrouperTargetMembership.put(loginName, grouperTargetMemberships);
      }
      
      grouperTargetMemberships.add(grouperTargetMembership);
      
    }
    RuntimeException runtimeException = null;
    for (String loginName : loginNames) {
      
      Set<String> groupNamesToInsert = GrouperUtil.nonNull(loginNameToGroupNamesToInsert.get(loginName));
      Set<String> groupNamesToDelete = GrouperUtil.nonNull(loginNameToGroupNamesToDelete.get(loginName));
      Set<ProvisioningMembership> grouperTargetMemberships = loginNameToGrouperTargetMembership.get(loginName);
      
      try {
        GrouperDigitalMarketplaceConfiguration digitalMarketplaceConfiguration = (GrouperDigitalMarketplaceConfiguration) this.getGrouperProvisioner().retrieveGrouperProvisioningConfiguration();
        
        String digitalMarketplaceExternalSystemConfigId = digitalMarketplaceConfiguration.getDigitalMarketplaceExternalSystemConfigId();
        
        GrouperDigitalMarketplaceApiCommands.updateMembershipsForDigitalMarketplaceUser(digitalMarketplaceExternalSystemConfigId, 
            loginName, groupNamesToInsert, groupNamesToDelete);
        
        for (ProvisioningMembership grouperTargetMembership : grouperTargetMemberships) {
          grouperTargetMembership.setProvisioned(true);
          for (ProvisioningObjectChange provisioningObjectChange : GrouperUtil.nonNull(grouperTargetMembership.getInternal_objectChanges())) {
            provisioningObjectChange.setProvisioned(true);
          }
        }
      } catch (GrouperDigitalMarketplaceUserDoesNotExist digitalMarketplaceUserDoesNotExist) {
        for (ProvisioningMembership grouperTargetMembership : grouperTargetMemberships) {
          grouperTargetMembership.getProvisioningMembershipWrapper().setErrorCode(GcGrouperSyncErrorCode.DNE);
        }
      } catch (RuntimeException e) {
        GrouperUtil.injectInException(e, "Update memberships for '" + loginName + "', inserts: " + GrouperUtil.join(groupNamesToInsert.iterator(), ",")
          + ", deletes: " + GrouperUtil.join(groupNamesToDelete.iterator(), ","));
        runtimeException = e;
        for (ProvisioningMembership grouperTargetMembership : grouperTargetMemberships) {
          grouperTargetMembership.setProvisioned(false);
          for (ProvisioningObjectChange provisioningObjectChange : GrouperUtil.nonNull(grouperTargetMembership.getInternal_objectChanges())) {
            provisioningObjectChange.setProvisioned(false);
          }
        }
        
      } finally {
        this.addTargetDaoTimingInfo(new TargetDaoTimingInfo("updateMembershipsForUser", startNanos));
      }

    }
    if (runtimeException != null) {
      throw runtimeException;
    }
    return new TargetDaoSendMembershipChangesToTargetResponse();
  }

  

  private TargetDaoRetrieveAllEntitiesResponse retrieveAllEntitiesHelper(
      TargetDaoRetrieveAllEntitiesRequest targetDaoRetrieveAllEntitiesRequest) {
    
    long startNanos = System.nanoTime();

    try {
      
      GrouperDigitalMarketplaceConfiguration digitalMarketplaceConfiguration = (GrouperDigitalMarketplaceConfiguration) this.getGrouperProvisioner().retrieveGrouperProvisioningConfiguration();
      String digitalMarketplaceExternalSystemConfigId = digitalMarketplaceConfiguration.getDigitalMarketplaceExternalSystemConfigId();
      
      List<ProvisioningEntity> results = new ArrayList<ProvisioningEntity>();
      
      Map<ProvisioningEntity, Object> targetEntityToNativeEntity = new HashMap<>();
      
      Map<String, GrouperDigitalMarketplaceUser> digitalMarketplaceUsers = GrouperDigitalMarketplaceApiCommands.retrieveDigitalMarketplaceUsers(digitalMarketplaceExternalSystemConfigId);

      for (GrouperDigitalMarketplaceUser grouperDigitalMarketplaceUser : digitalMarketplaceUsers.values()) {
        ProvisioningEntity targetEntity = grouperDigitalMarketplaceUser.toProvisioningEntity();
        targetEntityToNativeEntity.put(targetEntity, grouperDigitalMarketplaceUser);
        results.add(targetEntity);
      }
  
      TargetDaoRetrieveAllEntitiesResponse allEntitiesResponse = new TargetDaoRetrieveAllEntitiesResponse(results);
      allEntitiesResponse.setTargetEntityToTargetNativeEntity(targetEntityToNativeEntity);
      return allEntitiesResponse;
    } finally {
      this.addTargetDaoTimingInfo(new TargetDaoTimingInfo("retrieveAllEntities", startNanos));
    }
    
  }
  

  private synchronized TargetDaoRetrieveAllGroupsResponse retrieveAllGroupsHelper(TargetDaoRetrieveAllGroupsRequest targetDaoRetrieveAllGroupsRequest) {
    
    long startNanos = System.nanoTime();

    try {
      
      GrouperDigitalMarketplaceConfiguration digitalMarketplaceConfiguration = (GrouperDigitalMarketplaceConfiguration) this.getGrouperProvisioner().retrieveGrouperProvisioningConfiguration();
      
      String digitalMarketplaceExternalSystemConfigId = digitalMarketplaceConfiguration.getDigitalMarketplaceExternalSystemConfigId();
      
      List<ProvisioningGroup> results = new ArrayList<ProvisioningGroup>();
      
      Map<String, GrouperDigitalMarketplaceGroup> digitalMarketplaceGroups = GrouperDigitalMarketplaceApiCommands.retrieveDigitalMarketplaceGroups(digitalMarketplaceExternalSystemConfigId);

      for (GrouperDigitalMarketplaceGroup grouperDigitalMarketplaceGroup : digitalMarketplaceGroups.values()) {
        ProvisioningGroup targetGroup = grouperDigitalMarketplaceGroup.toProvisioningGroup();
        results.add(targetGroup);
      }
  
      return new TargetDaoRetrieveAllGroupsResponse(results);
    } finally {
      this.addTargetDaoTimingInfo(new TargetDaoTimingInfo("retrieveAllGroups", startNanos));
    }
  }
  
  @Override
  public TargetDaoRetrieveAllMembershipsResponse retrieveAllMemberships(TargetDaoRetrieveAllMembershipsRequest targetDaoRetrieveAllMembershipsRequest) {
    long startNanos = System.nanoTime();

    try {
      GrouperDigitalMarketplaceConfiguration digitalMarketplaceConfiguration = (GrouperDigitalMarketplaceConfiguration) this.getGrouperProvisioner().retrieveGrouperProvisioningConfiguration();
      String digitalMarketplaceExternalSystemConfigId = digitalMarketplaceConfiguration.getDigitalMarketplaceExternalSystemConfigId();
      
      Map<String, GrouperDigitalMarketplaceUser> marketplaceUsers = GrouperDigitalMarketplaceApiCommands.retrieveDigitalMarketplaceUsers(digitalMarketplaceExternalSystemConfigId);
      
      List<ProvisioningMembership> results = new ArrayList<>();
      
      for (GrouperDigitalMarketplaceUser user: marketplaceUsers.values()) {
        for (String groupName : user.getGroups()) {
          ProvisioningMembership targetMembership = new ProvisioningMembership();
          targetMembership.assignAttributeValue("groupName", groupName);
          targetMembership.assignAttributeValue("loginName", user.getLoginName());
          results.add(targetMembership);
        }
      }


      return new TargetDaoRetrieveAllMembershipsResponse(results);
    } finally {
      this.addTargetDaoTimingInfo(new TargetDaoTimingInfo("retrieveAllMemberships", startNanos));
    }
  }
  
  @Override
  public TargetDaoRetrieveEntityResponse retrieveEntity(TargetDaoRetrieveEntityRequest targetDaoRetrieveEntityRequest) {
    
    long startNanos = System.nanoTime();

    try {      
      GrouperDigitalMarketplaceConfiguration digitalMarketplaceConfiguration = (GrouperDigitalMarketplaceConfiguration) this.getGrouperProvisioner().retrieveGrouperProvisioningConfiguration();
      String digitalMarketplaceExternalSystemConfigId = digitalMarketplaceConfiguration.getDigitalMarketplaceExternalSystemConfigId();
      
      // we can only retrieve by login id
      GrouperDigitalMarketplaceUser grouperDigitalMarketplaceUser = null;

      if (StringUtils.equals("loginName", targetDaoRetrieveEntityRequest.getSearchAttribute())) {
        grouperDigitalMarketplaceUser = GrouperDigitalMarketplaceApiCommands.retrieveDigitalMarketplaceUser(
            digitalMarketplaceExternalSystemConfigId,
            GrouperUtil.stringValue(targetDaoRetrieveEntityRequest.getSearchAttributeValue()));
      }  else {
        throw new RuntimeException("Not expecting search attribute '" + targetDaoRetrieveEntityRequest.getSearchAttribute() + "'");
      }
      
      ProvisioningEntity targetEntity = grouperDigitalMarketplaceUser == null ? null
          : grouperDigitalMarketplaceUser.toProvisioningEntity();

      TargetDaoRetrieveEntityResponse targetDaoRetrieveEntityResponse = new TargetDaoRetrieveEntityResponse(targetEntity);
      if (targetDaoRetrieveEntityRequest.isIncludeNativeEntity()) {
        targetDaoRetrieveEntityResponse.setTargetNativeEntity(grouperDigitalMarketplaceUser);
      }
      return targetDaoRetrieveEntityResponse;
      
    } finally {
      this.addTargetDaoTimingInfo(new TargetDaoTimingInfo("retrieveEntity", startNanos));
    }
  }
  
  @Override
  public TargetDaoRetrieveGroupResponse retrieveGroup(TargetDaoRetrieveGroupRequest targetDaoRetrieveGroupRequest) {
    
    long startNanos = System.nanoTime();

    try {      
      GrouperDigitalMarketplaceConfiguration digitalMarketplaceConfiguration = (GrouperDigitalMarketplaceConfiguration) this.getGrouperProvisioner().retrieveGrouperProvisioningConfiguration();
      String digitalMarketplaceExternalSystemConfigId = digitalMarketplaceConfiguration.getDigitalMarketplaceExternalSystemConfigId();
      
      // we can only retrieve by permission group id
      GrouperDigitalMarketplaceGroup grouperDigitalMarketplaceGroup = null;

      if (StringUtils.equals("groupName", targetDaoRetrieveGroupRequest.getSearchAttribute())) {
        grouperDigitalMarketplaceGroup = GrouperDigitalMarketplaceApiCommands.retrieveDigitalMarketplaceGroup(
            digitalMarketplaceExternalSystemConfigId,
            GrouperUtil.stringValue(targetDaoRetrieveGroupRequest.getSearchAttributeValue()));
      }  else {
        throw new RuntimeException("Not expecting search attribute '" + targetDaoRetrieveGroupRequest.getSearchAttribute() + "'");
      }
      
      ProvisioningGroup targetGroup = grouperDigitalMarketplaceGroup == null ? null
          : grouperDigitalMarketplaceGroup.toProvisioningGroup();

      TargetDaoRetrieveGroupResponse targetDaoRetrieveGroupResponse = new TargetDaoRetrieveGroupResponse(targetGroup);
      targetDaoRetrieveGroupResponse.setTargetNativeGroup(grouperDigitalMarketplaceGroup);
      return targetDaoRetrieveGroupResponse;
      
    } finally {
      this.addTargetDaoTimingInfo(new TargetDaoTimingInfo("retrieveEntity", startNanos));
    }
  }
  
  
  /**
   * try to find group id from the target 
   * @param targetGroup
   * @return
   */
  private String resolveTargetGroupId(ProvisioningGroup targetGroup) {
    
    if (targetGroup == null) {
      return null;
    }
    
    if (StringUtils.isNotBlank(targetGroup.getId())) {
      return targetGroup.getId();
    }
    
    TargetDaoRetrieveGroupsRequest targetDaoRetrieveGroupsRequest = new TargetDaoRetrieveGroupsRequest();
    targetDaoRetrieveGroupsRequest.setTargetGroups(GrouperUtil.toList(targetGroup));
    targetDaoRetrieveGroupsRequest.setIncludeAllMembershipsIfApplicable(false);
    TargetDaoRetrieveGroupsResponse targetDaoRetrieveGroupsResponse = this.getGrouperProvisioner().retrieveGrouperProvisioningTargetDaoAdapter().retrieveGroups(
        targetDaoRetrieveGroupsRequest);

    if (targetDaoRetrieveGroupsResponse == null || GrouperUtil.length(targetDaoRetrieveGroupsResponse.getTargetGroups()) == 0) {
      return null;
    }
    
    return targetDaoRetrieveGroupsResponse.getTargetGroups().get(0).getId();
    
  }
  
  /**
   * try to find entity id from the target 
   * @param targetEntity
   * @return
   */
  private String resolveTargetEntityId(ProvisioningEntity targetEntity) {
    
    if (targetEntity == null) {
      return null;
    }
    
    if (StringUtils.isNotBlank(targetEntity.getId())) {
      return targetEntity.getId();
    }
    
    TargetDaoRetrieveEntitiesRequest targetDaoRetrieveEntitiesRequest = new TargetDaoRetrieveEntitiesRequest();
    targetDaoRetrieveEntitiesRequest.setTargetEntities(GrouperUtil.toList(targetEntity));
    targetDaoRetrieveEntitiesRequest.setIncludeAllMembershipsIfApplicable(false);
    
    TargetDaoRetrieveEntitiesResponse targetDaoRetrieveEntitiesResponse = this.getGrouperProvisioner().retrieveGrouperProvisioningTargetDaoAdapter().retrieveEntities(
        targetDaoRetrieveEntitiesRequest);

    if (targetDaoRetrieveEntitiesResponse == null || GrouperUtil.length(targetDaoRetrieveEntitiesResponse.getTargetEntities()) == 0) {
      return null;
    }
    
    return targetDaoRetrieveEntitiesResponse.getTargetEntities().get(0).getId();
  }
  
  @Override
  public TargetDaoRetrieveMembershipsByEntityResponse retrieveMembershipsByEntity(TargetDaoRetrieveMembershipsByEntityRequest targetDaoRetrieveMembershipsByEntityRequest) {
    long startNanos = System.nanoTime();
    ProvisioningEntity targetEntity = targetDaoRetrieveMembershipsByEntityRequest.getTargetEntity();
    
    String targetEntityId = resolveTargetEntityId(targetEntity);
    List<ProvisioningMembership> provisioningMemberships = new ArrayList<ProvisioningMembership>();
    
    if (StringUtils.isBlank(targetEntityId)) {
      return new TargetDaoRetrieveMembershipsByEntityResponse(provisioningMemberships);
    }
    
    try {
      GrouperDigitalMarketplaceConfiguration digitalMarketplaceConfiguration = (GrouperDigitalMarketplaceConfiguration) this.getGrouperProvisioner().retrieveGrouperProvisioningConfiguration();
      String digitalMarketplaceExternalSystemConfigId = digitalMarketplaceConfiguration.getDigitalMarketplaceExternalSystemConfigId();
      
      GrouperDigitalMarketplaceUser digitalMarketplaceUser = GrouperDigitalMarketplaceApiCommands.retrieveDigitalMarketplaceUser(digitalMarketplaceExternalSystemConfigId, targetEntityId);
      
      if (digitalMarketplaceUser == null) {
        return new TargetDaoRetrieveMembershipsByEntityResponse(provisioningMemberships);
      }
      
      for (String group : digitalMarketplaceUser.getGroups()) {
        
        ProvisioningMembership targetMembership = new ProvisioningMembership();
        
        targetMembership.assignAttributeValue("groupName", group);
        targetMembership.assignAttributeValue("loginName", targetEntityId);
        
        provisioningMemberships.add(targetMembership);
      }
      
      return new TargetDaoRetrieveMembershipsByEntityResponse(provisioningMemberships);
      
    } finally {
      this.addTargetDaoTimingInfo(new TargetDaoTimingInfo("retrieveMembershipsByEntity", startNanos));
    }
  }
  

  @Override
  public TargetDaoDeleteGroupResponse deleteGroup(
      TargetDaoDeleteGroupRequest targetDaoDeleteGroupRequest) {
    
    long startNanos = System.nanoTime();
    
    try {
      GrouperDigitalMarketplaceConfiguration digitalMarketplaceConfiguration = (GrouperDigitalMarketplaceConfiguration) this.getGrouperProvisioner().retrieveGrouperProvisioningConfiguration();
      String digitalMarketplaceExternalSystemConfigId = digitalMarketplaceConfiguration.getDigitalMarketplaceExternalSystemConfigId();
      ProvisioningGroup targetGroup = targetDaoDeleteGroupRequest.getTargetGroup();
      String groupName = null;
      if (targetGroup != null && StringUtils.isNotBlank(targetGroup.retrieveAttributeValueString("groupName"))) {
        groupName = targetGroup.retrieveAttributeValueString("groupName");
      }
      if (StringUtils.isBlank(groupName)) {
        return new TargetDaoDeleteGroupResponse();
      }
      Boolean removed = GrouperDigitalMarketplaceApiCommands.deleteDigitalMarketplaceGroup(digitalMarketplaceExternalSystemConfigId, groupName);
      if (removed != null) {
        targetGroup.setProvisioned(true);
        for (ProvisioningObjectChange provisioningObjectChange : GrouperUtil.nonNull(targetGroup.getInternal_objectChanges())) {
          provisioningObjectChange.setProvisioned(true);
        }
      }
      
      return new TargetDaoDeleteGroupResponse();
      
    } finally {
      this.addTargetDaoTimingInfo(new TargetDaoTimingInfo("deleteGroup", startNanos));
    }
      
  }
  
  @Override
  public TargetDaoInsertGroupResponse insertGroup(
      TargetDaoInsertGroupRequest targetDaoInsertGroupRequest) {
    
    long startNanos = System.nanoTime();
    
    try {
      GrouperDigitalMarketplaceConfiguration digitalMarketplaceConfiguration = (GrouperDigitalMarketplaceConfiguration) this.getGrouperProvisioner().retrieveGrouperProvisioningConfiguration();
      String digitalMarketplaceExternalSystemConfigId = digitalMarketplaceConfiguration.getDigitalMarketplaceExternalSystemConfigId();
      ProvisioningGroup targetGroup = targetDaoInsertGroupRequest.getTargetGroup();
      String comments = targetGroup.retrieveAttributeValueString("comments");
      String longGroupName = targetGroup.retrieveAttributeValueString("longGroupName");
      String groupName = targetGroup.retrieveAttributeValueString("groupName");
      String groupType = targetGroup.retrieveAttributeValueString("groupType");
      GrouperDigitalMarketplaceApiCommands.createDigitalMarketplaceGroup(digitalMarketplaceExternalSystemConfigId,
          groupName, longGroupName, comments, groupType);
        
      targetGroup.setProvisioned(true);
      for (ProvisioningObjectChange provisioningObjectChange : GrouperUtil.nonNull(targetGroup.getInternal_objectChanges())) {
        provisioningObjectChange.setProvisioned(true);
      }
      return new TargetDaoInsertGroupResponse();
    } finally {
      this.addTargetDaoTimingInfo(new TargetDaoTimingInfo("insertGroup", startNanos));
    }
    
  }

  @Override
  public TargetDaoRetrieveAllDataResponse retrieveAllData(
      TargetDaoRetrieveAllDataRequest targetDaoRetrieveAllDataRequest) {
    
    long startNanos = System.nanoTime();
    
    try {
      
      TargetDaoRetrieveAllGroupsResponse allGroups = retrieveAllGroupsHelper(new TargetDaoRetrieveAllGroupsRequest());
      TargetDaoRetrieveAllEntitiesResponse allEntities = retrieveAllEntitiesHelper(new TargetDaoRetrieveAllEntitiesRequest());
      
      Map<ProvisioningEntity, Object> targetEntityToTargetNativeEntity = allEntities.getTargetEntityToTargetNativeEntity();
      
      List<ProvisioningMembership> allMemberships = new ArrayList<>();
      
      for (ProvisioningEntity provisioningEntity : allEntities.getTargetEntities()) {
        GrouperDigitalMarketplaceUser grouperDigitalMarketplaceUser = (GrouperDigitalMarketplaceUser)targetEntityToTargetNativeEntity.get(provisioningEntity);
        
        for (String groupName : grouperDigitalMarketplaceUser.getGroups()) {
          ProvisioningMembership targetMembership = new ProvisioningMembership();
          targetMembership.assignAttributeValue("groupName", groupName);
          targetMembership.assignAttributeValue("loginName", grouperDigitalMarketplaceUser.getLoginName());
          allMemberships.add(targetMembership);
        }
        
      }
      
      GrouperProvisioningLists provisioningLists = new GrouperProvisioningLists();
      provisioningLists.setProvisioningGroups(allGroups.getTargetGroups());
      provisioningLists.setProvisioningEntities(allEntities.getTargetEntities());
      provisioningLists.setProvisioningMemberships(allMemberships);
      
      TargetDaoRetrieveAllDataResponse allDataResponse = new TargetDaoRetrieveAllDataResponse();
      allDataResponse.setTargetData(provisioningLists);
      
      return allDataResponse;
    } finally {
      this.addTargetDaoTimingInfo(new TargetDaoTimingInfo("retrieveAllData", startNanos));
    }
    
  }

  @Override
  public void registerGrouperProvisionerDaoCapabilities(
      GrouperProvisionerDaoCapabilities grouperProvisionerDaoCapabilities) {
   
    grouperProvisionerDaoCapabilities.setCanInsertGroup(true);
    grouperProvisionerDaoCapabilities.setCanDeleteGroup(true);
    
    grouperProvisionerDaoCapabilities.setCanRetrieveAllData(true);

    // it does this through send membership changes...    
    grouperProvisionerDaoCapabilities.setCanDeleteMembership(true);
    grouperProvisionerDaoCapabilities.setCanInsertMembership(true);

    grouperProvisionerDaoCapabilities.setCanSendMembershipChangesToTarget(true);

//    grouperProvisionerDaoCapabilities.setCanRetrieveAllEntities(true);
//    grouperProvisionerDaoCapabilities.setCanRetrieveAllGroups(true);
//    grouperProvisionerDaoCapabilities.setCanRetrieveAllMemberships(true);

    grouperProvisionerDaoCapabilities.setCanRetrieveEntity(true);
    grouperProvisionerDaoCapabilities.setCanRetrieveGroup(true);
    
    grouperProvisionerDaoCapabilities.setCanRetrieveMembershipsAllByEntity(true);
    
  }

}

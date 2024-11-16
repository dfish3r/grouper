package edu.internet2.middleware.grouper.sqlCache;

import edu.internet2.middleware.grouper.attr.assign.AttributeAssign;
import edu.internet2.middleware.grouper.attr.assign.AttributeAssignType;
import edu.internet2.middleware.grouper.hooks.AttributeAssignHooks;
import edu.internet2.middleware.grouper.hooks.beans.HooksAttributeAssignBean;
import edu.internet2.middleware.grouper.hooks.beans.HooksContext;
import edu.internet2.middleware.grouper.hooks.logic.GrouperHookType;
import edu.internet2.middleware.grouper.hooks.logic.GrouperHooksUtils;
import edu.internet2.middleware.grouper.hooks.logic.HookVeto;

/**
 * @author shilen
 */
public class SqlCacheMembershipHstAssignVetoHook extends AttributeAssignHooks {

  /**
   * 
   */
  public static void clearHook() {
    registered = false;
  }

  /**
   * if this hook is registered
   */
  private static boolean registered = false;
  
  public static void registerHookIfNecessary() {
    
    if (registered) {
      return;
    }
    
    GrouperHooksUtils.addHookManual(GrouperHookType.ATTRIBUTE_ASSIGN.getPropertyFileKey(), SqlCacheMembershipHstAssignVetoHook.class);
    
    registered = true;
  }

  /**
   * 
   */
  public SqlCacheMembershipHstAssignVetoHook() {
  }

  @Override
  public void attributeAssignPreInsert(HooksContext hooksContext, HooksAttributeAssignBean preInsertBean) {
    AttributeAssign attributeAssign = preInsertBean.getAttributeAssign();
    AttributeAssignType ownerType = attributeAssign.getAttributeAssignType();
    String attributeDefNameName = attributeAssign.getAttributeDefName().getName();
    
    if (attributeDefNameName.startsWith(SqlCacheGroup.attributeDefFolderName() + ":")) {
      if (attributeDefNameName.equals(SqlCacheGroup.sqlCacheableHistoryGroupMembersAttributeName()) ||
          attributeDefNameName.equals(SqlCacheGroup.sqlCacheableHistoryGroupAdminsAttributeName()) ||
          attributeDefNameName.equals(SqlCacheGroup.sqlCacheableHistoryGroupReadersAttributeName()) ||
          attributeDefNameName.equals(SqlCacheGroup.sqlCacheableHistoryGroupUpdatersAttributeName()) ||
          attributeDefNameName.equals(SqlCacheGroup.sqlCacheableHistoryGroupViewersAttributeName()) ||
          attributeDefNameName.equals(SqlCacheGroup.sqlCacheableHistoryGroupOptoutsAttributeName()) ||
          attributeDefNameName.equals(SqlCacheGroup.sqlCacheableHistoryGroupOptinsAttributeName()) ||
          attributeDefNameName.equals(SqlCacheGroup.sqlCacheableHistoryGroupAttrReadersAttributeName()) ||
          attributeDefNameName.equals(SqlCacheGroup.sqlCacheableHistoryGroupAttrUpdatersAttributeName())) {
        if (ownerType != AttributeAssignType.group) {
          throw new HookVeto("hook.veto.attributeAssign.mshipHistory.group", "This attribute can only be assigned to groups");     
        }
      }
      
      if (attributeDefNameName.equals(SqlCacheGroup.sqlCacheableHistoryAttributeDefAdminsAttributeName()) ||
          attributeDefNameName.equals(SqlCacheGroup.sqlCacheableHistoryAttributeDefReadersAttributeName()) ||
          attributeDefNameName.equals(SqlCacheGroup.sqlCacheableHistoryAttributeDefUpdatersAttributeName()) ||
          attributeDefNameName.equals(SqlCacheGroup.sqlCacheableHistoryAttributeDefViewersAttributeName()) ||
          attributeDefNameName.equals(SqlCacheGroup.sqlCacheableHistoryAttributeDefOptoutsAttributeName()) ||
          attributeDefNameName.equals(SqlCacheGroup.sqlCacheableHistoryAttributeDefOptinsAttributeName()) ||
          attributeDefNameName.equals(SqlCacheGroup.sqlCacheableHistoryAttributeDefAttrReadersAttributeName()) ||
          attributeDefNameName.equals(SqlCacheGroup.sqlCacheableHistoryAttributeDefAttrUpdatersAttributeName())) {
        if (ownerType != AttributeAssignType.attr_def) {
          throw new HookVeto("hook.veto.attributeAssign.mshipHistory.attributeDef", "This attribute can only be assigned to attributeDefs");     
        }
      }
      
      if (attributeDefNameName.equals(SqlCacheGroup.sqlCacheableHistoryStemAdminsAttributeName()) ||
          attributeDefNameName.equals(SqlCacheGroup.sqlCacheableHistoryStemCreatorsAttributeName()) ||
          attributeDefNameName.equals(SqlCacheGroup.sqlCacheableHistoryStemViewersAttributeName()) ||
          attributeDefNameName.equals(SqlCacheGroup.sqlCacheableHistoryStemAttrReadersAttributeName()) ||
          attributeDefNameName.equals(SqlCacheGroup.sqlCacheableHistoryStemAttrUpdatersAttributeName())) {
        if (ownerType != AttributeAssignType.stem) {
          throw new HookVeto("hook.veto.attributeAssign.mshipHistory.stem", "This attribute can only be assigned to folders");     
        }
      }
    }
  }
}

package edu.internet2.middleware.grouper.sqlCache;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import edu.internet2.middleware.grouper.Field;
import edu.internet2.middleware.grouper.FieldFinder;
import edu.internet2.middleware.grouper.cfg.GrouperConfig;
import edu.internet2.middleware.grouper.dictionary.GrouperDictionary;
import edu.internet2.middleware.grouper.tableIndex.TableIndex;
import edu.internet2.middleware.grouper.tableIndex.TableIndexType;
import edu.internet2.middleware.grouperClient.jdbc.GcDbVersionable;
import edu.internet2.middleware.grouperClient.jdbc.GcPersist;
import edu.internet2.middleware.grouperClient.jdbc.GcPersistableClass;
import edu.internet2.middleware.grouperClient.jdbc.GcPersistableField;
import edu.internet2.middleware.grouperClient.jdbc.GcSqlAssignPrimaryKey;
import edu.internet2.middleware.grouperClient.util.GrouperClientUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;

@GcPersistableClass(tableName="grouper_sql_cache_group", defaultFieldPersist=GcPersist.doPersist)
public class SqlCacheGroup implements GcSqlAssignPrimaryKey, GcDbVersionable {

  public SqlCacheGroup() {
    
  }

  /**
   * extension of folder
   */
  public static String attributeDefFolderExtension = "sqlCacheable";
 
  private static String attributeDefFolderName = null;
  
  public static String attributeDefFolderName() {
    
    if (attributeDefFolderName == null) {
      attributeDefFolderName = GrouperConfig.retrieveConfig().propertyValueString("grouper.rootStemForBuiltinObjects") + ":" + SqlCacheGroup.attributeDefFolderExtension;
    }
    
    return attributeDefFolderName;
  }
  
  public static String sqlCacheableHistoryDefExtension = "sqlCacheableHistoryDef";
  public static String sqlCacheableHistoryDefName() {
      return attributeDefFolderName() + ":" + SqlCacheGroup.sqlCacheableHistoryDefExtension;
  }
  
  public static String sqlCacheableHistoryGroupMembersAttributeExtension = "sqlCacheableHistoryGroupMembers";
  public static String sqlCacheableHistoryGroupMembersAttributeName() {    
      return attributeDefFolderName() + ":" + SqlCacheGroup.sqlCacheableHistoryGroupMembersAttributeExtension;
  }
  
  public static String sqlCacheableHistoryGroupAdminsAttributeExtension = "sqlCacheableHistoryGroupAdmins";
  public static String sqlCacheableHistoryGroupAdminsAttributeName() {    
      return attributeDefFolderName() + ":" + SqlCacheGroup.sqlCacheableHistoryGroupAdminsAttributeExtension;
  }
  
  public static String sqlCacheableHistoryGroupOptoutsAttributeExtension = "sqlCacheableHistoryGroupOptouts";
  public static String sqlCacheableHistoryGroupOptoutsAttributeName() {    
      return attributeDefFolderName() + ":" + SqlCacheGroup.sqlCacheableHistoryGroupOptoutsAttributeExtension;
  }
  
  public static String sqlCacheableHistoryGroupOptinsAttributeExtension = "sqlCacheableHistoryGroupOptins";
  public static String sqlCacheableHistoryGroupOptinsAttributeName() {    
      return attributeDefFolderName() + ":" + SqlCacheGroup.sqlCacheableHistoryGroupOptinsAttributeExtension;
  }
  
  public static String sqlCacheableHistoryGroupReadersAttributeExtension = "sqlCacheableHistoryGroupReaders";
  public static String sqlCacheableHistoryGroupReadersAttributeName() {    
      return attributeDefFolderName() + ":" + SqlCacheGroup.sqlCacheableHistoryGroupReadersAttributeExtension;
  }
  
  public static String sqlCacheableHistoryGroupUpdatersAttributeExtension = "sqlCacheableHistoryGroupUpdaters";
  public static String sqlCacheableHistoryGroupUpdatersAttributeName() {    
      return attributeDefFolderName() + ":" + SqlCacheGroup.sqlCacheableHistoryGroupUpdatersAttributeExtension;
  }
  
  public static String sqlCacheableHistoryGroupViewersAttributeExtension = "sqlCacheableHistoryGroupViewers";
  public static String sqlCacheableHistoryGroupViewersAttributeName() {    
      return attributeDefFolderName() + ":" + SqlCacheGroup.sqlCacheableHistoryGroupViewersAttributeExtension;
  }
  
  public static String sqlCacheableHistoryGroupAttrReadersAttributeExtension = "sqlCacheableHistoryGroupAttrReaders";
  public static String sqlCacheableHistoryGroupAttrReadersAttributeName() {    
      return attributeDefFolderName() + ":" + SqlCacheGroup.sqlCacheableHistoryGroupAttrReadersAttributeExtension;
  }
  
  public static String sqlCacheableHistoryGroupAttrUpdatersAttributeExtension = "sqlCacheableHistoryGroupAttrUpdaters";
  public static String sqlCacheableHistoryGroupAttrUpdatersAttributeName() {    
      return attributeDefFolderName() + ":" + SqlCacheGroup.sqlCacheableHistoryGroupAttrUpdatersAttributeExtension;
  }
  
  
  public static String sqlCacheableHistoryAttributeDefAdminsAttributeExtension = "sqlCacheableHistoryAttributeDefAdmins";
  public static String sqlCacheableHistoryAttributeDefAdminsAttributeName() {    
      return attributeDefFolderName() + ":" + SqlCacheGroup.sqlCacheableHistoryAttributeDefAdminsAttributeExtension;
  }
  
  public static String sqlCacheableHistoryAttributeDefOptoutsAttributeExtension = "sqlCacheableHistoryAttributeDefOptouts";
  public static String sqlCacheableHistoryAttributeDefOptoutsAttributeName() {    
      return attributeDefFolderName() + ":" + SqlCacheGroup.sqlCacheableHistoryAttributeDefOptoutsAttributeExtension;
  }
  
  public static String sqlCacheableHistoryAttributeDefOptinsAttributeExtension = "sqlCacheableHistoryAttributeDefOptins";
  public static String sqlCacheableHistoryAttributeDefOptinsAttributeName() {    
      return attributeDefFolderName() + ":" + SqlCacheGroup.sqlCacheableHistoryAttributeDefOptinsAttributeExtension;
  }
  
  public static String sqlCacheableHistoryAttributeDefReadersAttributeExtension = "sqlCacheableHistoryAttributeDefReaders";
  public static String sqlCacheableHistoryAttributeDefReadersAttributeName() {    
      return attributeDefFolderName() + ":" + SqlCacheGroup.sqlCacheableHistoryAttributeDefReadersAttributeExtension;
  }
  
  public static String sqlCacheableHistoryAttributeDefUpdatersAttributeExtension = "sqlCacheableHistoryAttributeDefUpdaters";
  public static String sqlCacheableHistoryAttributeDefUpdatersAttributeName() {    
      return attributeDefFolderName() + ":" + SqlCacheGroup.sqlCacheableHistoryAttributeDefUpdatersAttributeExtension;
  }
  
  public static String sqlCacheableHistoryAttributeDefViewersAttributeExtension = "sqlCacheableHistoryAttributeDefViewers";
  public static String sqlCacheableHistoryAttributeDefViewersAttributeName() {    
      return attributeDefFolderName() + ":" + SqlCacheGroup.sqlCacheableHistoryAttributeDefViewersAttributeExtension;
  }
  
  public static String sqlCacheableHistoryAttributeDefAttrReadersAttributeExtension = "sqlCacheableHistoryAttributeDefAttrReaders";
  public static String sqlCacheableHistoryAttributeDefAttrReadersAttributeName() {    
      return attributeDefFolderName() + ":" + SqlCacheGroup.sqlCacheableHistoryAttributeDefAttrReadersAttributeExtension;
  }
  
  public static String sqlCacheableHistoryAttributeDefAttrUpdatersAttributeExtension = "sqlCacheableHistoryAttributeDefAttrUpdaters";
  public static String sqlCacheableHistoryAttributeDefAttrUpdatersAttributeName() {    
      return attributeDefFolderName() + ":" + SqlCacheGroup.sqlCacheableHistoryAttributeDefAttrUpdatersAttributeExtension;
  }
  
  
  
  public static String sqlCacheableHistoryStemCreatorsAttributeExtension = "sqlCacheableHistoryStemCreators";
  public static String sqlCacheableHistoryStemCreatorsAttributeName() {    
      return attributeDefFolderName() + ":" + SqlCacheGroup.sqlCacheableHistoryStemCreatorsAttributeExtension;
  }
  
  public static String sqlCacheableHistoryStemAdminsAttributeExtension = "sqlCacheableHistoryStemAdmins";
  public static String sqlCacheableHistoryStemAdminsAttributeName() {    
      return attributeDefFolderName() + ":" + SqlCacheGroup.sqlCacheableHistoryStemAdminsAttributeExtension;
  }
  
  public static String sqlCacheableHistoryStemViewersAttributeExtension = "sqlCacheableHistoryStemViewers";
  public static String sqlCacheableHistoryStemViewersAttributeName() {    
      return attributeDefFolderName() + ":" + SqlCacheGroup.sqlCacheableHistoryStemViewersAttributeExtension;
  }
  
  public static String sqlCacheableHistoryStemAttrReadersAttributeExtension = "sqlCacheableHistoryStemAttrReaders";
  public static String sqlCacheableHistoryStemAttrReadersAttributeName() {    
      return attributeDefFolderName() + ":" + SqlCacheGroup.sqlCacheableHistoryStemAttrReadersAttributeExtension;
  }
  
  public static String sqlCacheableHistoryStemAttrUpdatersAttributeExtension = "sqlCacheableHistoryStemAttrUpdaters";
  public static String sqlCacheableHistoryStemAttrUpdatersAttributeName() {    
      return attributeDefFolderName() + ":" + SqlCacheGroup.sqlCacheableHistoryStemAttrUpdatersAttributeExtension;
  }
  
  private static Map<String, Field> sqlCacheHistoryAttributesToFields = null;
  
  public static Map<String, Field> getSqlCacheHistoryAttributeNamesToFields() {
    if (sqlCacheHistoryAttributesToFields == null) {
      synchronized(SqlCacheGroup.class) {
        if (sqlCacheHistoryAttributesToFields == null) {
          Map<String, Field> temp = new HashMap<>();
          temp.put(sqlCacheableHistoryGroupMembersAttributeName(), FieldFinder.find("members", true));
          temp.put(sqlCacheableHistoryGroupAdminsAttributeName(), FieldFinder.find("admins", true));
          temp.put(sqlCacheableHistoryGroupOptoutsAttributeName(), FieldFinder.find("optouts", true));
          temp.put(sqlCacheableHistoryGroupOptinsAttributeName(), FieldFinder.find("optins", true));
          temp.put(sqlCacheableHistoryGroupReadersAttributeName(), FieldFinder.find("readers", true));
          temp.put(sqlCacheableHistoryGroupUpdatersAttributeName(), FieldFinder.find("updaters", true));
          temp.put(sqlCacheableHistoryGroupViewersAttributeName(), FieldFinder.find("viewers", true));
          temp.put(sqlCacheableHistoryGroupAttrReadersAttributeName(), FieldFinder.find("groupAttrReaders", true));
          temp.put(sqlCacheableHistoryGroupAttrUpdatersAttributeName(), FieldFinder.find("groupAttrUpdaters", true));
          
          temp.put(sqlCacheableHistoryAttributeDefAdminsAttributeName(), FieldFinder.find("attrAdmins", true));
          temp.put(sqlCacheableHistoryAttributeDefOptoutsAttributeName(), FieldFinder.find("attrOptouts", true));
          temp.put(sqlCacheableHistoryAttributeDefOptinsAttributeName(), FieldFinder.find("attrOptins", true));
          temp.put(sqlCacheableHistoryAttributeDefReadersAttributeName(), FieldFinder.find("attrReaders", true));
          temp.put(sqlCacheableHistoryAttributeDefUpdatersAttributeName(), FieldFinder.find("attrUpdaters", true));
          temp.put(sqlCacheableHistoryAttributeDefViewersAttributeName(), FieldFinder.find("attrViewers", true));
          temp.put(sqlCacheableHistoryAttributeDefAttrReadersAttributeName(), FieldFinder.find("attrDefAttrReaders", true));
          temp.put(sqlCacheableHistoryAttributeDefAttrUpdatersAttributeName(), FieldFinder.find("attrDefAttrUpdaters", true));

          temp.put(sqlCacheableHistoryStemCreatorsAttributeName(), FieldFinder.find("creators", true));
          temp.put(sqlCacheableHistoryStemAdminsAttributeName(), FieldFinder.find("stemAdmins", true));
          temp.put(sqlCacheableHistoryStemViewersAttributeName(), FieldFinder.find("stemViewers", true));
          temp.put(sqlCacheableHistoryStemAttrReadersAttributeName(), FieldFinder.find("stemAttrReaders", true));
          temp.put(sqlCacheableHistoryStemAttrUpdatersAttributeName(), FieldFinder.find("stemAttrUpdaters", true));
        
          sqlCacheHistoryAttributesToFields = temp;
        }
      }
    }
    
    return Collections.unmodifiableMap(sqlCacheHistoryAttributesToFields);
  }

  public SqlCacheGroup getDbVersion() {
    return this.dbVersion;
  }
  
  /**
   * version from db
   */
  @GcPersistableField(persist = GcPersist.dontPersist)
  private SqlCacheGroup dbVersion;
  
  /**
   * take a snapshot of the data since this is what is in the db
   */
  @Override
  public void dbVersionReset() {
    //lets get the state from the db so we know what has changed
    this.dbVersion = this.clone();
  }

  /**
   * if we need to update this object
   * @return if needs to update this object
   */
  @Override
  public boolean dbVersionDifferent() {
    return !this.equalsDeep(this.dbVersion);
  }

  /**
   * db version
   */
  @Override
  public void dbVersionDelete() {
    this.dbVersion = null;
  }

  public void storePrepare() {
    if (this.createdOn == null) {
      this.createdOn = new Timestamp(System.currentTimeMillis());
    }
    
  }

  /**
   * deep clone the fields in this object
   */
  @Override
  public SqlCacheGroup clone() {

    SqlCacheGroup sqlCacheGroup = new SqlCacheGroup();
  
    //dbVersion  DONT CLONE
  
    sqlCacheGroup.createdOn = this.createdOn;
    sqlCacheGroup.disabledOn = this.disabledOn;
    sqlCacheGroup.fieldInternalId = this.fieldInternalId;
    sqlCacheGroup.groupInternalId = this.groupInternalId;
    sqlCacheGroup.internalId = this.internalId;
    sqlCacheGroup.membershipSize = this.membershipSize;
    sqlCacheGroup.membershipSizeHst = this.membershipSizeHst;
  
    return sqlCacheGroup;
  }

  /**
   *
   */
  public boolean equalsDeep(Object obj) {
    if (this==obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof GrouperDictionary)) {
      return false;
    }
    SqlCacheGroup other = (SqlCacheGroup) obj;

    return new EqualsBuilder()


      //dbVersion  DONT EQUALS
      .append(this.createdOn, other.createdOn)
      .append(this.disabledOn, other.disabledOn)
      .append(this.enabledOn, other.enabledOn)
      .append(this.fieldInternalId, other.fieldInternalId)
      .append(this.groupInternalId, other.groupInternalId)
      .append(this.internalId, other.internalId)
      .append(this.membershipSize, other.membershipSize)
      .append(this.membershipSizeHst, other.membershipSizeHst)
        .isEquals();

  }

  /**
   * internal integer id
   */
  @GcPersistableField(primaryKey=true, primaryKeyManuallyAssigned=true)
  private long internalId = -1;
  
  /**
   * internal integer id
   * @return
   */
  public long getInternalId() {
    return internalId;
  }

  /**
   * internal integer id
   * @param internalId
   */
  public void setInternalId(long internalId) {
    this.internalId = internalId;
  }

  /**
   * refers to the group being cached
   */
  private long groupInternalId = -1;


  /**
   * refers to the group being cached
   * @return
   */
  public long getGroupInternalId() {
    return groupInternalId;
  }

  /**
   * refers to the group being cached
   * @param groupInternalId
   */
  public void setGroupInternalId(long groupInternalId) {
    this.groupInternalId = groupInternalId;
  }

  /**
   * refers to the field of the group being cached
   */
  private long fieldInternalId = -1;

  /**
   * refers to the field of the group being cached
   * @return
   */
  public long getFieldInternalId() {
    return fieldInternalId;
  }

  /**
   * refers to the field of the group being cached
   * @param fieldInternalId
   */
  public void setFieldInternalId(long fieldInternalId) {
    this.fieldInternalId = fieldInternalId;
  }

  /**
   * number of members approximately for this list.  Note:
   * two incrementals at the same time could skew it
   */
  private long membershipSize = -1;

  /**
   * number of members approximately for this list.  Note:
   * two incrementals at the same time could skew it
   * @return
   */
  public long getMembershipSize() {
    return membershipSize;
  }

  /**
   * number of members approximately for this list.  Note:
   * two incrementals at the same time could skew it
   * @param membershipSize
   */
  public void setMembershipSize(long membershipSize) {
    this.membershipSize = membershipSize;
  }

  /**
   * number of records approximately for this group in the grouper hst memberships table
   * Note: two increments at the same time could skew it
   */
  private long membershipSizeHst = -1;


  /**
   * number of records approximately for this group in the grouper hst memberships table
   * Note: two increments at the same time could skew it
   * @return
   */
  public long getMembershipSizeHst() {
    return membershipSizeHst;
  }

  /**
   * number of records approximately for this group in the grouper hst memberships table
   * Note: two increments at the same time could skew it
   * @param membershipSizeHst
   */
  public void setMembershipSizeHst(long membershipSizeHst) {
    this.membershipSizeHst = membershipSizeHst;
  }

  /**
   * store the internal id to use when the db access stores the object
   */
  @GcPersistableField(persist = GcPersist.dontPersist)
  private Long tempInternalIdOnDeck = null;

  /**
   * store the internal id to use when the db access stores the object
   * @return
   */
  public Long getTempInternalIdOnDeck() {
    return tempInternalIdOnDeck;
  }

  /**
   * store the internal id to use when the db access stores the object
   * @param tempInternalIdOnDeck
   */
  public void setTempInternalIdOnDeck(Long tempInternalIdOnDeck) {
    this.tempInternalIdOnDeck = tempInternalIdOnDeck;
  }

  /**
   * 
   */
  @Override
  public boolean gcSqlAssignNewPrimaryKeyForInsert() {
    if (this.internalId != -1) {
      return false;
    }
    if (this.tempInternalIdOnDeck != null) {
      this.internalId = this.tempInternalIdOnDeck;
    } else {
      this.internalId = TableIndex.reserveId(TableIndexType.sqlGroupCache);
    }
    return true;
  }

  /**
   * when this row was created
   */
  private Timestamp createdOn;
  
  /**
   * when this row was created
   * @return
   */
  public Timestamp getCreatedOn() {
    return createdOn;
  }

  /**
   * when this row was created
   * @param createdOn
   */
  public void setCreatedOn(Timestamp createdOn) {
    this.createdOn = createdOn;
  }

  
  /**
   * when this row is ready to use by consumers (once the memberships are loaded)
   */
  private Timestamp enabledOn;

  /**
   * when this row is ready to use by consumers (once the memberships are loaded)
   * @return
   */
  public Timestamp getEnabledOn() {
    return enabledOn;
  }

  /**
   * when this row is ready to use by consumers (once the memberships are loaded)
   * @param enabledOn
   */
  public void setEnabledOn(Timestamp enabledOn) {
    this.enabledOn = enabledOn;
  }


  /**
   * when this shouldnt be used any more by consumers (before deletion)
   */
  private Timestamp disabledOn;
  
  /**
   * when this shouldnt be used any more by consumers (before deletion)
   * @return
   */
  public Timestamp getDisabledOn() {
    return disabledOn;
  }

  /**
   * when this shouldnt be used any more by consumers (before deletion)
   * @param disabledOn
   */
  public void setDisabledOn(Timestamp disabledOn) {
    this.disabledOn = disabledOn;
  }

  /**
   * 
   */
  @Override
  public String toString() {
    return GrouperClientUtils.toStringReflection(this, null);
  }

  /** table name for sql cache */
  public static final String TABLE_GROUPER_SQL_CACHE_GROUP = "grouper_sql_cache_group";
  
  /** created on col in db */
  public static final String COLUMN_CREATED_ON = "created_on";

  /** disabled on col in db */
  public static final String COLUMN_DISABLED_ON = "disabled_on";

  /** enabled on col in db */
  public static final String COLUMN_ENABLED_ON = "enabled_on";

  /** field internal id on col in db */
  public static final String COLUMN_FIELD_INTERNAL_ID = "field_internal_id";

  /** group internal id on col in db */
  public static final String COLUMN_GROUP_INTERNAL_ID = "group_internal_id";

  /** internal id on col in db */
  public static final String COLUMN_INTERNAL_ID = "internal_id";

  /** membership size on col in db */
  public static final String COLUMN_MEMBERSHIP_SIZE = "membership_size";

  /** membership size on col in db */
  public static final String COLUMN_MEMBERSHIP_SIZE_HST = "membership_size_hst";

  
}

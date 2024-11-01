package edu.internet2.middleware.grouper.sqlCache;

import edu.internet2.middleware.grouper.dictionary.GrouperDictionary;
import edu.internet2.middleware.grouperClient.jdbc.GcDbVersionable;
import edu.internet2.middleware.grouperClient.jdbc.GcPersist;
import edu.internet2.middleware.grouperClient.jdbc.GcPersistableClass;
import edu.internet2.middleware.grouperClient.jdbc.GcPersistableField;
import edu.internet2.middleware.grouperClient.util.GrouperClientUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;

@GcPersistableClass(tableName="grouper_sql_cache_mship_hst", defaultFieldPersist=GcPersist.doPersist)
public class SqlCacheMembershipHst implements GcDbVersionable {

  public SqlCacheMembershipHst() {
    
  }

  /**
   * version from db
   */
  @GcPersistableField(persist = GcPersist.dontPersist)
  private SqlCacheMembershipHst dbVersion;
  
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

  public SqlCacheMembershipHst getDbVersion() {
    return this.dbVersion;
  }

  /**
   * db version
   */
  @Override
  public void dbVersionDelete() {
    this.dbVersion = null;
  }

  public void storePrepare() {
  }

  /**
   * deep clone the fields in this object
   */
  @Override
  public SqlCacheMembershipHst clone() {

    SqlCacheMembershipHst sqlCacheGroup = new SqlCacheMembershipHst();

    sqlCacheGroup.endTime = this.endTime;
    sqlCacheGroup.startTime = this.startTime;
    sqlCacheGroup.memberInternalId = this.memberInternalId;
    sqlCacheGroup.sqlCacheGroupInternalId = this.sqlCacheGroupInternalId;
  
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
    SqlCacheMembershipHst other = (SqlCacheMembershipHst) obj;

    return new EqualsBuilder()

      //dbVersion  DONT EQUALS
      .append(this.endTime, other.endTime)
      .append(this.startTime, other.startTime)
      .append(this.memberInternalId, other.memberInternalId)
      .append(this.sqlCacheGroupInternalId, other.sqlCacheGroupInternalId)
        .isEquals();

  }

  /**
   * when this flattened membership started
   */
  @GcPersistableField(compoundPrimaryKey=true, primaryKeyManuallyAssigned=true)
  private Long startTime;
  
  /**
   * when this flattened membership started
   * @return
   */
  public Long getStartTime() {
    return startTime;
  }

  /**
   * when this flattened membership started
   * @param startTime
   */
  public void setStartTime(Long startTime) {
    this.startTime = startTime;
  }

  /**
   * when this flattened membership ended
   */
  private Long endTime;

  
  
  /**
   * when this flattened membership ended
   * @return
   */
  public Long getEndTime() {
    return endTime;
  }

  /**
   * when this flattened membership ended
   * @param endTime
   */
  public void setEndTime(Long endTime) {
    this.endTime = endTime;
  }

  /**
   * internal id of the member of this group/list
   */
  @GcPersistableField(compoundPrimaryKey=true, primaryKeyManuallyAssigned=true)
  private Long memberInternalId;
  
  /**
   * internal id of the member of this group/list
   * @return
   */
  public Long getMemberInternalId() {
    return memberInternalId;
  }

  /**
   * internal id of the member of this group/list
   * @param memberInternalId
   */
  public void setMemberInternalId(Long memberInternalId) {
    this.memberInternalId = memberInternalId;
  }

  /**
   * refers to which group and list this membership refers to
   */
  @GcPersistableField(compoundPrimaryKey=true, primaryKeyManuallyAssigned=true)
  private Long sqlCacheGroupInternalId;
  
  /**
   * refers to which group and list this membership refers to
   * @return
   */
  public Long getSqlCacheGroupInternalId() {
    return sqlCacheGroupInternalId;
  }

  /**
   * refers to which group and list this membership refers to
   * @param sqlCacheGroupInternalId
   */
  public void setSqlCacheGroupInternalId(Long sqlCacheGroupInternalId) {
    this.sqlCacheGroupInternalId = sqlCacheGroupInternalId;
  }

  /**
   * 
   */
  @Override
  public String toString() {
    return GrouperClientUtils.toStringReflection(this, null);
  }

  /** table name for sql cache */
  public static final String TABLE_GROUPER_SQL_CACHE_MEMBERSHIP_HST = "grouper_sql_cache_mship_hst";
  
  /** when this membership ended col in db */
  public static final String COLUMN_END_TIME = "end_time";

  /** when this membership started col in db */
  public static final String COLUMN_START_TIME = "start_time";

  /** internal id of the member of this group/list */
  public static final String COLUMN_MEMBER_INTERNAL_ID = "member_internal_id";

  /** refers to which group and list this membership refers to */
  public static final String COLUMN_SQL_CACHE_GROUP_INTERNAL_ID = "sql_cache_group_internal_id";


}

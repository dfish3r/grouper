package edu.internet2.middleware.grouper.sqlCache;

import java.util.Collection;

import edu.internet2.middleware.grouper.util.GrouperUtil;
import edu.internet2.middleware.grouperClient.jdbc.GcDbAccess;
import edu.internet2.middleware.grouperClient.util.GrouperClientConfig;

/**
 * dao for sql cache dependencies
 * @author mchyzer
 *
 */
public class SqlCacheDependencyDao {


  public SqlCacheDependencyDao() {
  }

  /**
   * 
   * @return true if changed
   */
  public static boolean store(SqlCacheDependency sqlCacheDependency) {
    sqlCacheDependency.storePrepare();
    boolean changed = new GcDbAccess().storeToDatabase(sqlCacheDependency);
    return changed;
  }

  /**
   * @return number of changes
   */
  public static int store(Collection<SqlCacheDependency> sqlCacheDependencies) {
    if (GrouperUtil.length(sqlCacheDependencies) == 0) {
      return 0;
    }
    for (SqlCacheDependency sqlCacheDependency : sqlCacheDependencies) {
      sqlCacheDependency.storePrepare();
    }
    int batchSize = GrouperClientConfig.retrieveConfig().propertyValueInt("grouperClient.syncTableDefault.maxBindVarsInSelect", 900);
    return new GcDbAccess().storeBatchToDatabase(sqlCacheDependencies, batchSize);
  }

  /**
   * select grouper sync by id
   * @param id
   * @return the sql cache dependency
   */
  public static SqlCacheDependency retrieveByInternalId(Long id) {
    SqlCacheDependency sqlCacheDependency = new GcDbAccess()
        .sql("select * from grouper_sql_cache_dependency where internal_id = ?").addBindVar(id).select(SqlCacheDependency.class);
    return sqlCacheDependency;
  }
  

  /**
   * 
   * @param sqlCacheDependency
   */
  public static void delete(SqlCacheDependency sqlCacheDependency) {
    new GcDbAccess().deleteFromDatabase(sqlCacheDependency);
  }
}

package edu.internet2.middleware.grouper.sqlCache;

import java.sql.Connection;
import java.util.Collection;

import edu.internet2.middleware.grouper.util.GrouperUtil;
import edu.internet2.middleware.grouperClient.jdbc.GcDbAccess;
import edu.internet2.middleware.grouperClient.util.GrouperClientConfig;

/**
 * dao for sql cache mship hst
 * @author shilen
 *
 */
public class SqlCacheMembershipHstDao {


  public SqlCacheMembershipHstDao() {
  }

  /**
   * 
   * @param sqlCacheGroups
   * @param connection optionally pass connection to use
   * @param isInsert
   * @return number of changes
   */
  public static int store(Collection<SqlCacheMembershipHst> sqlCacheMembershipHsts, Connection connection, boolean isInsert, boolean retryBatchStoreFailures, boolean ignoreRetriedBatchStoreFailures) {
    if (GrouperUtil.length(sqlCacheMembershipHsts) == 0) {
      return 0;
    }
    for (SqlCacheMembershipHst sqlCacheMembershipHst : sqlCacheMembershipHsts) {
      sqlCacheMembershipHst.storePrepare();
    }
    int batchSize = GrouperClientConfig.retrieveConfig().propertyValueInt("grouperClient.syncTableDefault.maxBindVarsInSelect", 900);
    return new GcDbAccess().connection(connection)
        .isInsert(isInsert)
        .retryBatchStoreFailures(retryBatchStoreFailures)
        .ignoreRetriedBatchStoreFailures(ignoreRetriedBatchStoreFailures)
        .storeBatchToDatabase(sqlCacheMembershipHsts, batchSize);
  }
}

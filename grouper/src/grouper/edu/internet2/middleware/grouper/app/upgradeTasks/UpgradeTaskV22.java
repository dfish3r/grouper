package edu.internet2.middleware.grouper.app.upgradeTasks;

import edu.internet2.middleware.grouper.GrouperSession;
import edu.internet2.middleware.grouper.app.loader.OtherJobBase.OtherJobInput;
import edu.internet2.middleware.grouper.ddl.GrouperDdlUtils;
import edu.internet2.middleware.grouper.exception.GrouperSessionException;
import edu.internet2.middleware.grouper.hibernate.HibernateSession;
import edu.internet2.middleware.grouper.misc.GrouperSessionHandler;
import edu.internet2.middleware.grouperClient.jdbc.GcDbAccess;

public class UpgradeTaskV22 implements UpgradeTasksInterface {
  
  @Override
  public void updateVersionFromPrevious(OtherJobInput otherJobInput) {
    GrouperSession.internal_callbackRootGrouperSession(new GrouperSessionHandler() {
      
      @Override
      public Object callback(GrouperSession grouperSession) throws GrouperSessionException {
        
        if (!GrouperDdlUtils.assertTableThere(true, "grouper_pit_stems")) {
          return null;
        }
        
        if (!GrouperDdlUtils.assertColumnThere(true, "grouper_pit_stems", "source_id_index")) {
          if (GrouperDdlUtils.isOracle()) {
            HibernateSession.bySqlStatic().executeSql("ALTER TABLE GROUPER_PIT_STEMS ADD source_id_index NUMBER(38)");
          } else {
            HibernateSession.bySqlStatic().executeSql("ALTER TABLE grouper_pit_stems ADD COLUMN source_id_index BIGINT");
          }
          
          otherJobInput.getHib3GrouperLoaderLog().appendJobMessage(", added column grouper_pit_stems.source_id_index");
        }
       
        if (!GrouperDdlUtils.assertColumnThere(true, "grouper_pit_attribute_def", "source_id_index")) {
          if (GrouperDdlUtils.isOracle()) {
            HibernateSession.bySqlStatic().executeSql("ALTER TABLE GROUPER_PIT_ATTRIBUTE_DEF ADD source_id_index NUMBER(38)");
          } else {
            HibernateSession.bySqlStatic().executeSql("ALTER TABLE grouper_pit_attribute_def ADD COLUMN source_id_index BIGINT");
          }
          
          otherJobInput.getHib3GrouperLoaderLog().appendJobMessage(", added column grouper_pit_attribute_def.source_id_index");
        }
        
        if (!GrouperDdlUtils.assertIndexExists("grouper_pit_stems", "pit_stem_source_idindex_idx")) {
          HibernateSession.bySqlStatic().executeSql("CREATE INDEX pit_stem_source_idindex_idx ON grouper_pit_stems (source_id_index)");
          otherJobInput.getHib3GrouperLoaderLog().appendJobMessage(", added index pit_stem_source_idindex_idx");
        }
        
        if (!GrouperDdlUtils.assertIndexExists("grouper_pit_attribute_def", "pit_attrdef_source_idindex_idx")) {
          HibernateSession.bySqlStatic().executeSql("CREATE INDEX pit_attrdef_source_idindex_idx ON grouper_pit_attribute_def (source_id_index)");
          otherJobInput.getHib3GrouperLoaderLog().appendJobMessage(", added index pit_attrdef_source_idindex_idx");
        }
        
        new GcDbAccess().sql("update grouper_pit_stems  ps set source_id_index = (select s.id_index from grouper_stems  s where ps.source_id = s.id) where ps.source_id_index is null and ps.active='T'").executeSql();
        new GcDbAccess().sql("update grouper_pit_attribute_def  pad set source_id_index = (select ad.id_index from grouper_attribute_def ad where pad.source_id = ad.id) where pad.source_id_index is null and pad.active='T'").executeSql();
        
        return null;
      }
    });
  }

}

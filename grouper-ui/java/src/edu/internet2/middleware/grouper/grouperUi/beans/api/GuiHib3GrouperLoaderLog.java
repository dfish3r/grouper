/**
 * @author mchyzer
 * $Id$
 */
package edu.internet2.middleware.grouper.grouperUi.beans.api;

import java.util.ArrayList;
import java.util.List;

import edu.internet2.middleware.grouper.ui.GrouperUiFilter;
import org.apache.commons.lang.StringUtils;

import edu.internet2.middleware.grouper.Group;
import edu.internet2.middleware.grouper.GroupFinder;
import edu.internet2.middleware.grouper.GrouperSession;
import edu.internet2.middleware.grouper.app.loader.GrouperLoaderStatus;
import edu.internet2.middleware.grouper.app.loader.db.Hib3GrouperLoaderLog;
import edu.internet2.middleware.grouper.grouperUi.beans.ui.GrouperLoaderContainer;
import edu.internet2.middleware.grouper.ui.util.GrouperUiConfig;
import edu.internet2.middleware.grouper.util.GrouperUtil;
import java.util.Locale;

/**
 * gui class to display a loader log
 */
public class GuiHib3GrouperLoaderLog {

  /**
   * 
   * @param hib3GrouperLoaderLogs
   * @return hib3GrouperLoaderLogs
   */
  public static List<GuiHib3GrouperLoaderLog> convertFromHib3GrouperLoaderLogs(List<Hib3GrouperLoaderLog> hib3GrouperLoaderLogs) {
    return convertFromHib3GrouperLoaderLogs(hib3GrouperLoaderLogs, null, -1);
  }

  /**
   * gui group of group being loaded if applicable
   */
  private GuiGroup loadedGuiGroup;

  /**
   * get the loaded group, not null if there are multiple
   * @return the loaded group
   */
  public GuiGroup getLoadedGuiGroup() {

    if (this.loadedGuiGroup == null) {
    
      String jobName = null;
        
      //if there is a parent than this is a child
      if (!StringUtils.isBlank(this.hib3GrouperLoaderLog.getParentJobId())) {
  
        jobName = this.hib3GrouperLoaderLog.getJobName();
        
      } else {
        
        //see if this is a simple job
        /*
        // TODO why was this getting the job name so indirectly??
        GrouperLoaderType grouperLoaderType = GrouperRequestContainer.retrieveFromRequestOrCreate()
            .getGrouperLoaderContainer().getGrouperLoaderType();
        
        if (grouperLoaderType == GrouperLoaderType.SQL_SIMPLE || grouperLoaderType == GrouperLoaderType.LDAP_SIMPLE) {
          jobName = GrouperRequestContainer.retrieveFromRequestOrCreate()
              .getGrouperLoaderContainer().getJobName();
        } */
        
        // this could be run via the AdminContainer as well so don't rely on GrouperLoaderContainer being there
        if (this.hib3GrouperLoaderLog.getJobName() != null && 
            this.hib3GrouperLoaderLog.getJobName().startsWith("SQL_SIMPLE__") || this.hib3GrouperLoaderLog.getJobName().startsWith("LDAP_SIMPLE__")) {
          jobName = this.hib3GrouperLoaderLog.getJobName();
        }
        
      }
      
      if (jobName == null) { 
        return null;
      }
      
      String groupName = GrouperLoaderContainer.retrieveGroupNameFromJobName(jobName);
      
      //not sure why it would be null, but program defensively
      if (StringUtils.isBlank(groupName)) {
        
        return null;
        
      }
  
      Group group = GroupFinder.findByName(GrouperSession.staticGrouperSession(), groupName, false);
      
      if (group == null) {
        return null;
      }
      this.loadedGuiGroup = new GuiGroup(group);
    }
    
    return this.loadedGuiGroup;
  }
  
  /**
   * @return true if this is a job that would load one or more groups
   */
  public boolean isLoadedGroupJob() {
    String jobName = this.hib3GrouperLoaderLog.getJobName();
    
    if (jobName == null) {
      return false;
    }
    
    if (jobName.startsWith("SQL_SIMPLE__") || jobName.startsWith("LDAP_SIMPLE__") ||
        jobName.startsWith("SQL_GROUP_LIST__") || jobName.startsWith("LDAP_GROUP_LIST") ||
        jobName.startsWith("LDAP_GROUPS_FROM_ATTRIBUTES")) {
      return true;
    }
    
    return false;
  }
  
  /**
   * 
   * @return status background color
   */
  public String getStatusBackgroundColor() {
    
    GrouperLoaderStatus grouperLoaderStatus = GrouperLoaderStatus.ERROR;
    
    if (!StringUtils.isBlank(this.hib3GrouperLoaderLog.getStatus())) {
      
      grouperLoaderStatus = GrouperLoaderStatus.valueOfIgnoreCase(this.hib3GrouperLoaderLog.getStatus(), false);
      
    }
    
    if (grouperLoaderStatus == null) {
      
      grouperLoaderStatus = GrouperLoaderStatus.ERROR;
      
    }

    switch(grouperLoaderStatus) {
      case ERROR:
      case CONFIG_ERROR:
        return "red";
      case SUBJECT_PROBLEMS:
      case WARNING:
        return "orange";
      case RUNNING:
      case STARTED:
        return "yellow";
      case SUCCESS:
        return "green";
    }
    return "red";
  }
  
  /**
   * 
   * @return status background color
   */
  public String getStatusTextColor() {
    
    GrouperLoaderStatus grouperLoaderStatus = GrouperLoaderStatus.ERROR;
    
    if (!StringUtils.isBlank(this.hib3GrouperLoaderLog.getStatus())) {
      
      grouperLoaderStatus = GrouperLoaderStatus.valueOfIgnoreCase(this.hib3GrouperLoaderLog.getStatus(), false);
      
    }
    
    if (grouperLoaderStatus == null) {
      
      grouperLoaderStatus = GrouperLoaderStatus.ERROR;
      
    }

    switch(grouperLoaderStatus) {
      case ERROR:
      case CONFIG_ERROR:
        return "white";
      case SUBJECT_PROBLEMS:
      case WARNING:
        return "black";
      case RUNNING:
      case STARTED:
        return "black";
      case SUCCESS:
        return "white";
    }
    return "white";
  }
  
  /**
   * 
   * @param hib3GrouperLoaderLogs
   * @param configMax
   * @param defaultMax
   * @return groups
   */
  public static List<GuiHib3GrouperLoaderLog> convertFromHib3GrouperLoaderLogs(List<Hib3GrouperLoaderLog> hib3GrouperLoaderLogs, String configMax, int defaultMax) {
    List<GuiHib3GrouperLoaderLog> tempHib3LoaderLogs = new ArrayList<GuiHib3GrouperLoaderLog>();
    
    Integer max = null;
    
    if (!StringUtils.isBlank(configMax)) {
      max = GrouperUiConfig.retrieveConfig().propertyValueInt(configMax, defaultMax);
    }
    
    int count = 0;
    for (Hib3GrouperLoaderLog hib3GrouperLoaderLog : GrouperUtil.nonNull(hib3GrouperLoaderLogs)) {
      tempHib3LoaderLogs.add(new GuiHib3GrouperLoaderLog(hib3GrouperLoaderLog));
      if (max != null && ++count >= max) {
        break;
      }
    }
    
    return tempHib3LoaderLogs;
    
  }

  /**
   * @param hib3GrouperLoaderLog1
   */
  public GuiHib3GrouperLoaderLog(Hib3GrouperLoaderLog hib3GrouperLoaderLog1) {
    super();
    this.hib3GrouperLoaderLog = hib3GrouperLoaderLog1;
  }

  /**
   * return the encosed object
   * @return the log object
   */
  public Hib3GrouperLoaderLog getHib3GrouperLoaderLog() {
    return this.hib3GrouperLoaderLog;
  }
  
  /**
   * encloses this
   */
  private Hib3GrouperLoaderLog hib3GrouperLoaderLog;
  
  /**
   * 
   */
  public GuiHib3GrouperLoaderLog() {
  }

  private String convertMillisToTime(Integer millis) {
    String unit = GrouperUiConfig.retrieveConfig().propertyValueString("uiV2.admin.daemonJob.elapsedTimeUnit", "milliseconds");

    if (millis == null) {
      return null;
    }

    if (millis == 0) {
      return "0";
    }

    Locale locale = GrouperUiFilter.retrieveLocale();
    String result;

    switch (unit) {
      case "seconds":
        result = String.format(locale, "%.2f", millis / 1000f);
        break;
      case "minutes":
        result = String.format(locale, "%.2f", millis/60000f);
        break;
      case "h:m:s":
        long millis2 = millis;
        long hours2 = millis2 / (1000 * 60 * 60);
        millis2 %= (1000 * 60 * 60);
        long minutes2 = millis2 / (1000 * 60);
        millis2 %= (1000 * 60);
        long seconds2 = millis2 / 1000;
        long ms2 = millis2 % 1000;

        result = String.format("%d:%02d:%02d.%03d", hours2, minutes2, seconds2, ms2);
        break;
      case "hms":
        long millis3 = millis;
        long hours3 = millis3 / (1000 * 60 * 60);
        millis3 %= (1000 * 60 * 60);
        long minutes3 = millis3 / (1000 * 60);
        millis3 %= (1000 * 60);
        float seconds3 = millis3 / 1000f;

        StringBuffer buffer = new StringBuffer();
        if (hours3 > 0) {
          buffer.append(hours3 + "h");
        }
        if (hours3 > 0 || minutes3 > 0) {
          buffer.append(minutes3 + "m");
        }
        if (hours3 > 0 || minutes3 > 0 || seconds3 > 0) {
          buffer.append(String.format(locale, "%.2f", seconds3) + "s");
        } else {
          buffer.append(millis3 + "ms");
        }

        result = buffer.toString();
        break;
      case "milliseconds":
      default:
        result = millis.toString();
    }

    return result;
  }

  public String getTotalElapsedFormatted() {
    return convertMillisToTime(hib3GrouperLoaderLog.getMillis());
  }

  public String getGetDataElapsedFormatted() {
    return convertMillisToTime(hib3GrouperLoaderLog.getMillisGetData());
  }

  public String getLoadDataElapsedFormatted() {
    return convertMillisToTime(hib3GrouperLoaderLog.getMillisLoadData());
  }
}

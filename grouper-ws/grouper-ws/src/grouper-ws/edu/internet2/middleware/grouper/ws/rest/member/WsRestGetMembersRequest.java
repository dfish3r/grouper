/*******************************************************************************
 * Copyright 2012 Internet2
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
/*
 * @author mchyzer
 * $Id: WsRestGetMembersRequest.java,v 1.2 2009-12-07 07:31:14 mchyzer Exp $
 */
package edu.internet2.middleware.grouper.ws.rest.member;

import edu.internet2.middleware.grouper.ws.GrouperServiceLogic;
import edu.internet2.middleware.grouper.ws.coresoap.WsGroupLookup;
import edu.internet2.middleware.grouper.ws.coresoap.WsParam;
import edu.internet2.middleware.grouper.ws.coresoap.WsSubjectLookup;
import edu.internet2.middleware.grouper.ws.rest.WsRequestBean;
import edu.internet2.middleware.grouper.ws.rest.method.GrouperRestHttpMethod;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;

/**
 * bean that will be the data from rest request
 * @see GrouperServiceLogic#hasMember(edu.internet2.middleware.grouper.ws.GrouperWsVersion, WsGroupLookup, WsSubjectLookup[], edu.internet2.middleware.grouper.ws.member.WsMemberFilter, WsSubjectLookup, edu.internet2.middleware.grouper.Field, boolean, boolean, String[], WsParam[])
 * for method
 */
@ApiModel(description = "bean that will be the data from rest request for get members<br /><br /><b>actAsSubjectLookup</b>: If allowed to act as other users (e.g. if a UI uses the Grouper WS behind the scenes), specify the user to act as here<br />"
    + "<br /><br /><b>wsGroupLookups</b>: group to get members from<br />"
    + "<br /><br /><b>subjectLookups</b>: subjects to be inqured about<br />"
    + "<br /><br /><b>params</b>: optional params for this request<br />")
public class WsRestGetMembersRequest implements WsRequestBean {
  
  /** field */
  private String clientVersion;
  
  /** field */
  private WsGroupLookup[] wsGroupLookups;
  
  /** field */
  private String memberFilter;
  
  /** field */
  private WsSubjectLookup actAsSubjectLookup;
  
  /** field */
  private String fieldName;
  
  /** field */
  private String includeGroupDetail;
  
  /** field */
  private String includeSubjectDetail;
  
  /** field */
  private String[] subjectAttributeNames;
  
  /** field */
  private WsParam[] params;
  
  /** sourceIds, or null if all */
  private String[] sourceIds;
  
  /**
   * page size if paging
   */
  private String pageSize;
      
  /**
   * page number 1 indexed if paging
   */
  private String pageNumber;
  
  /**
   * sortString must be an hql query field, e.g. 
   * can sort on uuid, subjectId, sourceId, name, description, sortString0, sortString1, sortString2, sortString3, sortString4
   */
  private String sortString;
  
  /**
   * ascending T or null for ascending, F for descending.  
   */
  private String ascending;
  
  
  /**
   * T|F default to F.  if this is T then we are doing cursor paging
   */
  private String pageIsCursor;
  
  /**
   * field that will be sent back for cursor based paging
   */
  private String pageLastCursorField;
  
  /**
   * could be: string, int, long, date, timestamp
   */
  private String pageLastCursorFieldType;
  
  /**
   * T|F
   */
  private String pageCursorFieldIncludesLastRetrieved;
  
  /**
   * page size if paging
   * @return page size
   */
  @ApiModelProperty(value = "Page size if paging", example = "100")
  public String getPageSize() {
    return this.pageSize;
  }


  /**
   * page size if paging
   * @param pageSize1
   */
  public void setPageSize(String pageSize1) {
    this.pageSize = pageSize1;
  }


  /**
   * page number 1 indexed if paging
   * @return page number
   */
  @ApiModelProperty(value = "Page number 1 indexed if paging", example = "1")
  public String getPageNumber() {
    return this.pageNumber;
  }


  /**
   * page number 1 indexed if paging
   * @param pageNumber1
   */
  public void setPageNumber(String pageNumber1) {
    this.pageNumber = pageNumber1;
  }


  /**
   * sortString must be an hql query field, e.g. 
   * can sort on uuid, subjectId, sourceId, name, description, sortString0, sortString1, sortString2, sortString3, sortString4
   * @return sort string
   */
  @ApiModelProperty(value = "Must be an hql query field, e.g. can sort on name, displayName, extension, displayExtension", 
      example = "name | displayName | extension | displayExtension")
  public String getSortString() {
    return this.sortString;
  }


  /**
   * sortString must be an hql query field, e.g. 
   * can sort on uuid, subjectId, sourceId, name, description, sortString0, sortString1, sortString2, sortString3, sortString4
   * @param sortString1
   */
  public void setSortString(String sortString1) {
    this.sortString = sortString1;
  }


  /**
   * ascending T or null for ascending, F for descending.  
   * @return ascending
   */
  @ApiModelProperty(value = "T or null for ascending, F for descending.  If you pass true or false, must pass a sort string", example = "T|F")
  public String getAscending() {
    return this.ascending;
  }


  /**
   * ascending T or null for ascending, F for descending.  
   * @param ascending1
   */
  public void setAscending(String ascending1) {
    this.ascending = ascending1;
  }

  
  /**
   * T|F|null
   */
  private String pointInTimeRetrieve;
  
  
  /**
   * To query members at a certain point in time or time range in the past, set this value
   * and/or the value of pointInTimeTo.  This parameter specifies the start of the range
   * of the point in time query.  If this is specified but pointInTimeTo is not specified, 
   * then the point in time query range will be from the time specified to now.  
   * Format:  yyyy/MM/dd HH:mm:ss.SSS
   */
  private String pointInTimeFrom;
  
  /**
   * To query members at a certain point in time or time range in the past, set this value
   * and/or the value of pointInTimeFrom.  This parameter specifies the end of the range 
   * of the point in time query.  If this is the same as pointInTimeFrom, then the query 
   * will be done at a single point in time rather than a range.  If this is specified but 
   * pointInTimeFrom is not specified, then the point in time query range will be from the 
   * minimum point in time to the time specified.  Format: yyyy/MM/dd HH:mm:ss.SSS   
   */
  private String pointInTimeTo;
  
  
  /**
   * sourceIds, or null if all
   * @return the sourceIds
   */
  @ApiModelProperty(value = "sourceIds, or null if all", example = "a1b2, c3d4")
  public String[] getSourceIds() {
    return this.sourceIds;
  }
  
  /**
   * sourceIds, or null if all
   * @param sourceIds1 the sourceIds to set
   */
  public void setSourceIds(String[] sourceIds1) {
    this.sourceIds = sourceIds1;
  }

  /**
   * @return the clientVersion
   */
  @ApiModelProperty(value = "Version of the client (i.e. that the client was coded against)", example = "v2_6_001")
  public String getClientVersion() {
    return this.clientVersion;
  }

  
  /**
   * @param clientVersion1 the clientVersion to set
   */
  public void setClientVersion(String clientVersion1) {
    this.clientVersion = clientVersion1;
  }

  
  /**
   * @return the wsGroupLookups
   */
  public WsGroupLookup[] getWsGroupLookups() {
    return this.wsGroupLookups;
  }

  
  /**
   * @param wsGroupLookups1 the wsGroupLookup to set
   */
  public void setWsGroupLookups(WsGroupLookup[] wsGroupLookups1) {
    this.wsGroupLookups = wsGroupLookups1;
  }

  
  /**
   * @return the replaceAllExisting
   */
  @ApiModelProperty(value = "can be All(default), Effective (non immediate), Immediate (direct),Composite (if composite group with group math (union, minus,etc)", example = "Effective")
  public String getMemberFilter() {
    return this.memberFilter;
  }

  
  /**
   * @param replaceAllExisting1 the replaceAllExisting to set
   */
  public void setMemberFilter(String replaceAllExisting1) {
    this.memberFilter = replaceAllExisting1;
  }

  
  /**
   * @return the actAsSubjectLookup
   */
  public WsSubjectLookup getActAsSubjectLookup() {
    return this.actAsSubjectLookup;
  }

  
  /**
   * @param actAsSubjectLookup1 the actAsSubjectLookup to set
   */
  public void setActAsSubjectLookup(WsSubjectLookup actAsSubjectLookup1) {
    this.actAsSubjectLookup = actAsSubjectLookup1;
  }

  
  /**
   * @return the fieldName
   */
  @ApiModelProperty(value = "field name (list) to search, blank for groups list", example = "members, optin, optout, read, admin, update, view, groupAttrRead, groupAttrUpdate")
  public String getFieldName() {
    return this.fieldName;
  }

  
  /**
   * @param fieldName1 the fieldName to set
   */
  public void setFieldName(String fieldName1) {
    this.fieldName = fieldName1;
  }

  
  /**
   * @return the includeGroupDetail
   */
  @ApiModelProperty(value = "T or F for if more info about the group should be returned", example = "T|F")
  public String getIncludeGroupDetail() {
    return this.includeGroupDetail;
  }

  
  /**
   * @param includeGroupDetail1 the includeGroupDetail to set
   */
  public void setIncludeGroupDetail(String includeGroupDetail1) {
    this.includeGroupDetail = includeGroupDetail1;
  }

  
  /**
   * @return the includeSubjectDetail
   */
  @ApiModelProperty(value = "If the subject detail should be returned, default to false", example = "T|F")
  public String getIncludeSubjectDetail() {
    return this.includeSubjectDetail;
  }

  
  /**
   * @param includeSubjectDetail1 the includeSubjectDetail to set
   */
  public void setIncludeSubjectDetail(String includeSubjectDetail1) {
    this.includeSubjectDetail = includeSubjectDetail1;
  }

  
  /**
   * @return the subjectAttributeNames
   */
  @ApiModelProperty(value = "are the additional subject attributes (data) to return. If blank, whatever is configured in the grouper-ws.properties will be sent (comma separated). Only certain attributes are configured to be allowed to be retrieved", example = "lastName, middleName")
  public String[] getSubjectAttributeNames() {
    return this.subjectAttributeNames;
  }

  
  /**
   * @param subjectAttributeNames1 the subjectAttributeNames to set
   */
  public void setSubjectAttributeNames(String[] subjectAttributeNames1) {
    this.subjectAttributeNames = subjectAttributeNames1;
  }


  
  /**
   * @return the params
   */
  public WsParam[] getParams() {
    return this.params;
  }


  
  /**
   * @param params1 the params to set
   */
  public void setParams(WsParam[] params1) {
    this.params = params1;
  }

  /**
   * @see edu.internet2.middleware.grouper.ws.rest.WsRequestBean#retrieveRestHttpMethod()
   */
  @Override
  public GrouperRestHttpMethod retrieveRestHttpMethod() {
    return GrouperRestHttpMethod.GET;
  }
  
  /**
   * To query members at a certain point in time or time range in the past, set this value
   * and/or the value of pointInTimeTo.  This parameter specifies the start of the range
   * of the point in time query.  If this is specified but pointInTimeTo is not specified, 
   * then the point in time query range will be from the time specified to now.  
   * Format:  yyyy/MM/dd HH:mm:ss.SSS
   * @return the pointInTimeFrom
   */
  @ApiModelProperty(value = "To query members at a certain point in time or time range in the past, set this value and/or the value of pointInTimeTo.  This parameter specifies the start of the range of the point in time query.  If this is specified but pointInTimeTo is not specified, then the point in time query range will be from the time specified to now.", example = "1970/01/01 00:00:00.000")
  public String getPointInTimeFrom() {
    return this.pointInTimeFrom;
  }

  
  /**
   * To query members at a certain point in time or time range in the past, set this value
   * and/or the value of pointInTimeTo.  This parameter specifies the start of the range
   * of the point in time query.  If this is specified but pointInTimeTo is not specified, 
   * then the point in time query range will be from the time specified to now.  
   * Format:  yyyy/MM/dd HH:mm:ss.SSS
   * @param pointInTimeFrom1 the pointInTimeFrom to set
   */
  public void setPointInTimeFrom(String pointInTimeFrom1) {
    this.pointInTimeFrom = pointInTimeFrom1;
  }

  
  /**
   * To query members at a certain point in time or time range in the past, set this value
   * and/or the value of pointInTimeFrom.  This parameter specifies the end of the range 
   * of the point in time query.  If this is the same as pointInTimeFrom, then the query 
   * will be done at a single point in time rather than a range.  If this is specified but 
   * pointInTimeFrom is not specified, then the point in time query range will be from the 
   * minimum point in time to the time specified.  Format: yyyy/MM/dd HH:mm:ss.SSS 
   * @return the pointInTimeTo
   */
  @ApiModelProperty(value = "To query members at a certain point in time or time range in the past, set this value and/or the value of pointInTimeTo.  This parameter specifies the start of the range of the point in time query.  If this is specified but pointInTimeTo is not specified, then the point in time query range will be from the time specified to now.", example = "1970/01/01 00:00:00.000")
  public String getPointInTimeTo() {
    return this.pointInTimeTo;
  }

  
  /**
   * To query members at a certain point in time or time range in the past, set this value
   * and/or the value of pointInTimeFrom.  This parameter specifies the end of the range 
   * of the point in time query.  If this is the same as pointInTimeFrom, then the query 
   * will be done at a single point in time rather than a range.  If this is specified but 
   * pointInTimeFrom is not specified, then the point in time query range will be from the 
   * minimum point in time to the time specified.  Format: yyyy/MM/dd HH:mm:ss.SSS 
   * @param pointInTimeTo1 the pointInTimeTo to set
   */
  public void setPointInTimeTo(String pointInTimeTo1) {
    this.pointInTimeTo = pointInTimeTo1;
  }


  /**
   * @return the pageIsCursor
   */
  @ApiModelProperty(value = "T|F default to F.  if this is T then we are doing cursor paging", example = "T|F")
  public String getPageIsCursor() {
    return this.pageIsCursor;
  }


  /**
   * @param pageIsCursor1 the pageIsCursor to set
   */
  public void setPageIsCursor(String pageIsCursor1) {
    this.pageIsCursor = pageIsCursor1;
  }


  /**
   * @return the pageLastCursorField
   */
  @ApiModelProperty(value = "Field that will be sent back for cursor based paging", example = "abc123")
  public String getPageLastCursorField() {
    return this.pageLastCursorField;
  }


  /**
   * @param pageLastCursorField1 the pageLastCursorField to set
   */
  public void setPageLastCursorField(String pageLastCursorField1) {
    this.pageLastCursorField = pageLastCursorField1;
  }


  /**
   * @return the pageLastCursorFieldType
   */
  @ApiModelProperty(value = "Could be: string, int, long, date, timestamp", example = "string|int|long|date|timestamp")
  public String getPageLastCursorFieldType() {
    return this.pageLastCursorFieldType;
  }


  /**
   * @param pageLastCursorFieldType1 the pageLastCursorFieldType to set
   */
  public void setPageLastCursorFieldType(String pageLastCursorFieldType1) {
    this.pageLastCursorFieldType = pageLastCursorFieldType1;
  }


  /**
   * @return the pageCursorFieldIncludesLastRetrieved
   */
  @ApiModelProperty(value = "If cursor field is unique, this should be false.  If not, then should be true.  i.e. if should include the last cursor field in the next resultset", example = "T|F")
  public String getPageCursorFieldIncludesLastRetrieved() {
    return this.pageCursorFieldIncludesLastRetrieved;
  }


  /**
   * @param pageCursorFieldIncludesLastRetrieved1 the pageCursorFieldIncludesLastRetrieved to set
   */
  public void setPageCursorFieldIncludesLastRetrieved(String pageCursorFieldIncludesLastRetrieved1) {
    this.pageCursorFieldIncludesLastRetrieved = pageCursorFieldIncludesLastRetrieved1;
  }

  /**
   * T|F|null
   */
  @ApiModelProperty(value = "true means retrieve point in time records", example = "T|F|null")
  public String getPointInTimeRetrieve() {
    return this.pointInTimeRetrieve;
  }


  /**
   * T|F|null
   */
  public void setPointInTimeRetrieve(String pointInTimeRetrieve) {
    this.pointInTimeRetrieve = pointInTimeRetrieve;
  }
  
  
  
}

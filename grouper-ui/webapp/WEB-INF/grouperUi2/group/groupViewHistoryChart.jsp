<%@ include file="../assetsJsp/commonTaglib.jsp"%>

<script src="../../grouperExternal/public/assets/js/grouperMemberActivityChart.js" type="text/javascript"></script>

${grouper:titleFromKeyAndText('groupHistoryChartPageTitle', grouperRequestContainer.groupContainer.guiGroup.group.displayName)}

            <!-- start group/groupViewHistoryChart.jsp -->
            <%-- for the new group or new stem button --%>
            <input type="hidden" name="objectStemId" value="${grouperRequestContainer.groupContainer.guiGroup.group.parentUuid}" />

            <%@ include file="groupHeader.jsp" %>

            <div class="row-fluid">
              <div class="span12">
                <div id="messages"></div>
				<div class="tab-interface">
                  <ul class="nav nav-tabs">
                    <li><a role="tab" href="#" onclick="return guiV2link('operation=UiV2Group.viewGroup&groupId=${grouperRequestContainer.groupContainer.guiGroup.group.id}', {dontScrollTop: true});" >${textContainer.text['groupMembersTab'] }</a></li>
                    <c:if test="${grouperRequestContainer.groupContainer.canAdmin}">
                      <li><a role="tab" href="#" onclick="return guiV2link('operation=UiV2Group.groupPrivileges&groupId=${grouperRequestContainer.groupContainer.guiGroup.group.id}', {dontScrollTop: true});" >${textContainer.text['groupPrivilegesTab'] }</a></li>
                    </c:if>
                    <%@ include file="groupMoreTab.jsp" %>
                  </ul>
				</div>

                <p class="lead">${textContainer.text['groupHistoryChartGroupDescription'] }</p>

                <div class="row-fluid">
                  <div class="span12">
                    <form class="form-inline form-small form-filter" id="groupChartFormId" method="post"
                          action="../app/UiV2Group.viewHistoryChartResults?groupId=${grouperRequestContainer.groupContainer.guiGroup.group.id}">

                      <!-- time period type absolute/relative -->
                      <div class="row-fluid">
                        <div class="span1">
                          <label for="dateFromAbsoluteOptionId" style="white-space: nowrap;">${textContainer.text['groupHistoryChartRange'] }</label>
                        </div>
                        <div class="span4" style="white-space: nowrap;">
                          <a class="btn chartRangeOption" role="button" id="dateFromRelativeOptionId" aria-controls="dateFromRelativeOptionId" href="#" onclick="return showHideActivityChartFormDates(this)" data-html="true" data-delay-show="200" data-placement="right" rel="tooltip" data-original-title="${textContainer.text['groupHistoryChartBtnDateFromRelativeOptionTooltip'] }">${textContainer.text['groupHistoryChartBtnDateFromRelativeOption'] }</a>
                          <a class="btn chartRangeOption" role="button" id="dateFromAbsoluteOptionId" aria-controls="dateFromAbsoluteOptionId" href="#" onclick="return showHideActivityChartFormDates(this)" data-html="true" data-delay-show="200" data-placement="right" rel="tooltip" data-original-title="${textContainer.text['groupHistoryChartBtnDateFromAbsoluteOptionTooltip'] }">${textContainer.text['groupHistoryChartBtnDateFromAbsoluteOption'] }</a>
                          <input type="hidden" name="dateRangeType" id="dateRangeTypeId" />
                        </div>
                      </div>

                      <!-- relative span -->
                      <div id="date-range-relative-block-container" class="hide">
                        <div class="row-fluid">
                          <div class="span1">
                            <label for="dateFromRelativeId" style="white-space: nowrap;">${textContainer.text['groupHistoryChartTimePeriodFrom'] }</label>
                          </div>
                          <div class="span4" style="white-space: nowrap;">

                            <input type="text" name="dateFromRelative" id="dateFromRelativeId" class="span3" placeholder="${textContainer.text['groupHistoryChartRelativeScalePlaceholder'] }">

                            <select name="dateFromRelativeScale" id="dateFromRelativeScale" class="span4">
                              <option value="years" selected>${textContainer.text['groupHistoryChartRelativeScaleYears'] }</option>
                              <option value="months">${textContainer.text['groupHistoryChartRelativeScaleMonths'] }</option>
                              <option value="days" selected>${textContainer.text['groupHistoryChartRelativeScaleDays'] }</option>
                              <option value="hours">${textContainer.text['groupHistoryChartRelativeScaleHours'] }</option>
                            </select>
                          </div>
                        </div>
                      </div>

                      <div id="date-range-absolute-block-container" class="hide">
                        <!-- absolute span from -->
                          <div class="row-fluid">
                            <div class="span1">
                              <label for="dateFromAbsoluteId" style="white-space: nowrap;">${textContainer.text['groupHistoryChartTimePeriodFrom'] }</label>
                            </div>
                            <div class="span5" style="white-space: nowrap;">
                              <input type="datetime-local" step="1" class="span8" name="dateFromAbsolute"  placeholder="${textContainer.text['membershipEditDatePlaceholder'] }" id="dateFromAbsoluteId">
                            </div>
                          </div>

                          <!-- absolute span to -->
                          <div class="row-fluid">
                            <div class="span1">
                              <label for="dateToAbsoluteId" style="white-space: nowrap;">${textContainer.text['groupHistoryChartTimePeriodTo'] }</label>
                            </div>
                            <div class="span5" style="white-space: nowrap;">
                              <input type="datetime-local" step="1" class="span8" name="dateToAbsolute"  placeholder="${textContainer.text['membershipEditDatePlaceholder'] }" id="dateToAbsoluteId">
                            </div>
                          </div>
                      </div>

                      <input type="hidden" id="btnActionId" name="action" value="graph" />
                      <input type="hidden" name="<csrf:token-name/>" value="<csrf:token-value />"/><!-- needed for the export -->
                      <button type="submit" class="btn"
                              onclick="document.getElementById('btnActionId').value='graph'; ajax('../app/UiV2Group.viewHistoryChartResults?groupId=${grouperRequestContainer.groupContainer.guiGroup.group.id}', {formIds: 'groupChartFormId'}); return false;"
                      >${textContainer.text['groupHistoryChartActionShowChart']}</button>
                      <button type="submit" class="btn"
                              onclick="document.getElementById('btnActionId').value='export'; return true"
                      >${textContainer.text['groupHistoryChartActionExportData']}</button>
                    </form>

                  </div>
                </div>
                <script type="text/javascript">
                  $(document).ready(showHideActivityChartFormDates(document.getElementById('dateFromRelativeOptionId')))
                </script>

                <div id="grouperhistoryChartDivId" role="region" aria-live="polite">
                </div>
              </div>
            </div>
            <!-- end group/groupViewhistoryChart.jsp -->
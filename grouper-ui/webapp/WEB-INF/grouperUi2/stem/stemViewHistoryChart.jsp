<%@ include file="../assetsJsp/commonTaglib.jsp"%>

<script src="../../grouperExternal/public/assets/js/grouperMemberActivityChart.js" type="text/javascript"></script>

${grouper:titleFromKeyAndText('groupHistoryChartPageTitle', grouperRequestContainer.stemContainer.guiStem.stem.displayName)}

            <!-- start stem/stemViewHistoryChart.jsp -->
            <%-- for the new group or new stem button --%>
            <input type="hidden" name="objectStemId" value="${grouperRequestContainer.stemContainer.guiStem.stem.id}" />
            <%-- show the add member button for privileges --%>
            <c:set target="${grouperRequestContainer.stemContainer}" property="showAddMember" value="true" />
            <%@ include file="stemHeader.jsp" %>

            <div class="row-fluid">
              <div class="span12 tab-interface">
                <ul class="nav nav-tabs">
                  <li><a role="tab" href="#" onclick="return guiV2link('operation=UiV2Stem.viewStem&stemId=${grouperRequestContainer.stemContainer.guiStem.stem.id}', {dontScrollTop: true});" >${textContainer.text['stemContents'] }</a></li>
                  <li class="active"><a role="tab"  aria-selected="true" href="#" onclick="return false;" >${textContainer.text['stemPrivileges'] }</a></li>
                  <%@ include file="stemMoreTab.jsp" %>
                </ul>
				</div>

                <p class="lead">${textContainer.text['groupHistoryChartFolderDescription'] }</p>

                <div class="row-fluid">
                  <div class="span12">
                    <form class="form-inline form-small form-filter" id="stemChartFormId" method="post"
                          action="../app/UiV2Stem.viewHistoryChartResults?stemId=${grouperRequestContainer.stemContainer.guiStem.stem.id}">

                      <!-- time period type absolute/relative -->
                      <div class="row-fluid">
                        <div class="span1">
                          <label for="dateFromAbsoluteOptionId" style="white-space: nowrap;">${textContainer.text['groupHistoryChartRange'] }</label>
                        </div>
                        <div class="span4" style="white-space: nowrap;">
                          <a class="btn chartRangeOption" role="button" id="dateFromRelativeOptionId" aria-controls="dateFromRelativeOptionId" href="#" onclick="return showHideActivityChartFormDates(this)">${textContainer.text['groupHistoryChartBtnDateFromRelativeOption'] }</a>
                          <a class="btn chartRangeOption" role="button" id="dateFromAbsoluteOptionId" aria-controls="dateFromAbsoluteOptionId" href="#" onclick="return showHideActivityChartFormDates(this)">${textContainer.text['groupHistoryChartBtnDateFromAbsoluteOption'] }</a>
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
                              onclick="document.getElementById('btnActionId').value='graph'; ajax('../app/UiV2Stem.viewHistoryChartResults?stemId=${grouperRequestContainer.stemContainer.guiStem.stem.id}', {formIds: 'stemChartFormId'}); return false;"
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
            <!-- end stem/stemViewhistoryChart.jsp -->
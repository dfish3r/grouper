<%@ include file="../assetsJsp/commonTaglib.jsp"%>
${grouper:title('miscAttestationDataFieldAndRowDictionaryLink')}

<div class="bread-header-container">
  <ul class="breadcrumb">
    <li><a href="#" onclick="return guiV2link('operation=UiV2Main.indexMain');">${textContainer.text['myServicesHomeBreadcrumb'] }</a><span
      class="divider"><i class='fa fa-angle-right'></i></span></li>
    <li><a href="#" onclick="return guiV2link('operation=UiV2Main.miscellaneous');">${textContainer.text['miscellaneousBreadcrumb'] }</a><span
      class="divider"><i class='fa fa-angle-right'></i></span></li>
    <li class="active">${textContainer.text['miscAttestationDataFieldAndRowDictionaryLink'] }</li>
  </ul>

  <div class="page-header blue-gradient">

    <div class="row-fluid">
      <div class="lead span8 pull-left">
        <h4>${textContainer.text['miscAttestationDataFieldAndRowDictionaryLink'] }</h4>
      </div>
    </div>
    
    <div class="row-fluid">
      <div class="span9"> <p>${textContainer.text['dataFieldDictionaryPageDescription'] }</p></div>
    </div>
    
  </div>
</div>

<div class="row-fluid">
     
     <c:set var="j" value="0" />
     <c:forEach items="${grouperRequestContainer.entityDataFieldsContainer.guiDataFieldRowDictionaryTables}" var="guiDataFieldRowDictionaryTable">
      
      <div class="row-fluid">
         <div class="span12">
            <div class="control-group">
              <h4>${guiDataFieldRowDictionaryTable.title}</h4>
            </div>
            
            <div class="control-group">
              <span>${guiDataFieldRowDictionaryTable.description}</span>
            </div>
            
            <c:if test="${!grouper:isBlank(guiDataFieldRowDictionaryTable.documentation)}">
              <div class="control-group">
                 <p>             
                    <a href="#" onclick="$('#documentation-${j}').toggle('slow'); return false;">${textContainer.text['entityDataFieldRowDictionaryTableDocumentation']}</a>
                 </p>
                <span id="documentation-${j}" style="display: none;">${guiDataFieldRowDictionaryTable.documentation}</span>
              </div>
            </c:if>
            
        </div>
      </div>
      
      <c:choose>
        <c:when test="${guiDataFieldRowDictionaryTable.canAccess == false}">
          ${textContainer.text['entityDataFieldRowDictionaryTableNoViewAccess']}
        </c:when>

        <c:when test="${guiDataFieldRowDictionaryTable.guiDataFieldRowDictionary.size() == 0}">
          ${textContainer.text['entityDataFieldRowDictionaryTableNoData']}
        </c:when>

        <c:otherwise>
        
      <table
        class="table table-hover table-bordered table-striped table-condensed data-table">
        <thead>
          <tr>
            <th>${textContainer.text['entityDataFieldRowDictionaryHeaderDataFieldAliases']}</th> 
            <th>${textContainer.text['entityDataFieldRowDictionaryHeaderDescription']}</th>
            <th>${textContainer.text['entityDataFieldRowDictionaryHeaderPrivilege']}</th>
            <th>${textContainer.text['entityDataFieldRowDictionaryHeaderDataType']}</th>
            <th>${textContainer.text['entityDataFieldRowDictionaryHeaderDataOwner']}</th>
            <th>${textContainer.text['entityDataFieldRowDictionaryHeaderHowToGetAccess']}</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
         <c:set var="i" value="0" />
         <c:forEach items="${guiDataFieldRowDictionaryTable.guiDataFieldRowDictionary}" var="guiDataFieldRowDictionary">
              
            <tr>
              <td style="white-space: nowrap;">
                ${guiDataFieldRowDictionary.dataFieldAliases}
              </td>
              
              <td style="white-space: nowrap;">
                ${guiDataFieldRowDictionary.description}
              </td>
              
              <td style="white-space: nowrap;">
                ${guiDataFieldRowDictionary.privilege}
              </td>
              
              <td style="white-space: nowrap;">
                ${guiDataFieldRowDictionary.dataType}
              </td>
              
              <td style="white-space: nowrap;">
                ${guiDataFieldRowDictionary.dataOwner}
              </td>
              
              <td style="white-space: nowrap;">
                ${guiDataFieldRowDictionary.howToGetAccess}
              </td>
              
              <td style="white-space: nowrap;">
                <c:if test="${guiDataFieldRowDictionaryTable.dataField}">
                  <p>             
                    <a href="#" onclick="$('#about-datafield-${j}-${i}').slideToggle('slow'); return false;">${textContainer.text['entityDataFieldRowDictionaryHeaderAboutDataField']}</a>
                  </p>
                  <div class="row-fluid" id="about-datafield-${j}-${i}" style="display: none;">
                     <div class="span12">
                        <div class="control-group">
                          <label class="control-label" style="padding-top: 0px; display: inline;">${textContainer.text['entityDataFieldRowDictionaryValueType'] }: </label>
                          <span>${guiDataFieldRowDictionary.valueType}</span>
                        </div>
                        
                        <div class="control-group">
                          <label class="control-label" style="padding-top: 0px; display: inline;">${textContainer.text['entityDataFieldRowDictionaryValueTypeMultivalued'] }: </label>
                          <span>${guiDataFieldRowDictionary.multiValued}</span>
                        </div>
                    </div>
                  </div>
                </c:if>
                
                <c:if test="${!grouper:isBlank(guiDataFieldRowDictionary.examples)}">
                  <a href="#" onclick="$('#examples-${j}-${i}').toggle('slow'); return false;">${textContainer.text['entityDataFieldRowDictionaryHeaderExamples']}</a>
                  <div id="examples-${j}-${i}" style="display: none;">                
                    ${guiDataFieldRowDictionary.examples}
                  </div>
                </c:if>
              </td>
              </tr>
              <c:set var="i" value="${i+1}" />
         </c:forEach>
              
        </tbody>
       </table>
        </c:otherwise>
        
      </c:choose>
      
      <hr style="border: 1px solid black;">
      <c:set var="j" value="${j+1}" />
     </c:forEach>
</div>

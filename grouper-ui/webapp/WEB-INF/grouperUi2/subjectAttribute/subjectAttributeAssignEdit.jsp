<%@ include file="../assetsJsp/commonTaglib.jsp"%>

<div class="row-fluid">
  <div class="span12">
    
    <form id="simpleAttributeAssignEditForm" name="simpleAttributeAssignEditFormName" class="form-horizontal">
      
      <input name="subjectId" type="hidden" value="${attributeUpdateRequestContainer.guiAttributeAssign.attributeAssign.ownerMember.subjectId}" />
      <c:if test="${attributeUpdateRequestContainer.guiAttributeAssignAssign != null}">
        <input name="attributeAssignId" type="hidden" value="${attributeUpdateRequestContainer.guiAttributeAssignAssign.attributeAssign.id }" />
      </c:if>
      <c:if test="${attributeUpdateRequestContainer.guiAttributeAssignAssign == null}">
        <input name="attributeAssignId" type="hidden" value="${attributeUpdateRequestContainer.guiAttributeAssign.attributeAssign.id }" />
      </c:if>
      
      <div class="control-group">
        <c:set var="attributeAssignTypeLabelKey" value="${attributeUpdateRequestContainer.attributeAssignTypeLabelKey}"></c:set>
        <label class="control-label no-padding">${textContainer.text[attributeAssignTypeLabelKey] }
          <c:if test="${attributeUpdateRequestContainer.guiAttributeAssignAssign != null}">
            ${textContainer.text['simpleAttributeUpdate.assignMetadataLabelSuffix']}
          </c:if>
        </label>
        <div class="controls">
          <span>${grouper:escapeHtml(attributeUpdateRequestContainer.guiAttributeAssign.screenLabelLong)}</span>
        </div>
      </div>
      
      <div class="control-group">
        <label class="control-label no-padding">${textContainer.text['simpleAttributeUpdate.assignHeaderAttributeName'] }
          <c:if test="${attributeUpdateRequestContainer.guiAttributeAssignAssign != null}">
            ${textContainer.text['simpleAttributeUpdate.assignMetadataLabelSuffix'] }
          </c:if>
        </label>
        <div class="controls">
          <span>${attributeUpdateRequestContainer.guiAttributeAssign.guiAttributeDefName.shortLinkWithIcon}</span>
        </div>
      </div>
      
      <div class="control-group">
        <label class="control-label no-padding">${textContainer.text['simpleAttributeUpdate.assignHeaderAttributeDef'] }
          <c:if test="${attributeUpdateRequestContainer.guiAttributeAssignAssign != null}">
            ${textContainer.text['simpleAttributeUpdate.assignMetadataLabelSuffix'] }
          </c:if>
        </label>
        <div class="controls">
          <span>${attributeUpdateRequestContainer.guiAttributeAssign.guiAttributeDef.shortLinkWithIcon}</span>
        </div>
      </div>
      
      <div class="control-group">
        <label class="control-label no-padding">${textContainer.text['simpleAttributeAssign.assignEditId'] }</label>
        <div class="controls">
          <span>
            ${attributeUpdateRequestContainer.guiAttributeAssignAssign == null ? attributeUpdateRequestContainer.guiAttributeAssign.attributeAssign.id : attributeUpdateRequestContainer.guiAttributeAssignAssign.attributeAssign.id}
          </span>
        </div>
      </div>
      
      <div class="control-group">
        <label class="control-label no-padding">${textContainer.text['simpleAttributeUpdate.assignEditEnabledDate'] }</label>
        <div class="controls">
          <input type="datetime-local" step="1" name="enabledDate"  id="enabledDateId"
            value="${attributeUpdateRequestContainer.guiAttributeAssignAssign == null ? attributeUpdateRequestContainer.guiAttributeAssign.enabledDate : attributeUpdateRequestContainer.guiAttributeAssignAssign.enabledDate}" 
            />
        </div>
      </div>
      
      <div class="control-group">
        <label class="control-label no-padding">${textContainer.text['simpleAttributeUpdate.assignEditDisabledDate'] }</label>
        <div class="controls">
          <input type="datetime-local" step="1" name="disabledDate"  id="disabledDateId"
            value="${attributeUpdateRequestContainer.guiAttributeAssignAssign == null ? attributeUpdateRequestContainer.guiAttributeAssign.disabledDate : attributeUpdateRequestContainer.guiAttributeAssignAssign.disabledDate}" 
            />
        </div>
      </div>
      
      <div class="form-actions">
        <a href="#" class="btn btn-primary" role="button" onclick="ajax('../app/UiV2SubjectAttributeAssignment.assignEditSubmit', {formIds: 'simpleAttributeAssignEditForm'}); return false;">${textContainer.text['simpleAttributeAssign.assignEditSubmitButton'] }</a>
        <a href="#" onclick="return guiV2link('operation=UiV2SubjectAttributeAssignment.viewAttributeAssignments&subjectId=${attributeUpdateRequestContainer.guiAttributeAssign.attributeAssign.ownerMember.subjectId}');"
                           class="btn">${textContainer.text['simpleAttributeAssign.assignEditCancelButton']}</a> 
      </div>
      
    </form>
    
  </div>
</div>
/**
 * Copyright 2014 Internet2
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
 */
/*
  Copyright (C) 2007 University Corporation for Advanced Internet Development, Inc.
  Copyright (C) 2007 The University Of Chicago

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

package edu.internet2.middleware.grouper.internal.dao;

import  java.io.Writer;

/** 
 * Basic <code>Registry</code> DAO interface.
 * @author  blair christensen.
 * @version $Id: RegistryDAO.java,v 1.8 2008-11-06 21:51:22 mchyzer Exp $
 * @since   1.2.0
 */
public interface RegistryDAO extends GrouperDAO {

  /**
   * @param includeTypesincludeTypesAndFields 
   * @throws GrouperDAOException 
   * @since   1.2.0
   */
  public void reset(boolean includeTypesincludeTypesAndFields)
    throws  GrouperDAOException;

} 


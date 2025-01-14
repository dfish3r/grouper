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
  Copyright (C) 2004-2007 University Corporation for Advanced Internet Development, Inc.
  Copyright (C) 2004-2007 The University Of Chicago

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

package edu.internet2.middleware.grouper.membership;
import java.util.Date;

import junit.textui.TestRunner;

import org.apache.commons.logging.Log;

import edu.internet2.middleware.grouper.Field;
import edu.internet2.middleware.grouper.FieldFinder;
import edu.internet2.middleware.grouper.Group;
import edu.internet2.middleware.grouper.MembershipFinder;
import edu.internet2.middleware.grouper.helper.GrouperTest;
import edu.internet2.middleware.grouper.helper.R;
import edu.internet2.middleware.grouper.helper.T;
import edu.internet2.middleware.grouper.misc.CompositeType;
import edu.internet2.middleware.grouper.privs.AccessPrivilege;
import edu.internet2.middleware.grouper.util.GrouperUtil;
import edu.internet2.middleware.subject.Subject;

/**
 * @author Shilen Patel.
 */
public class TestMembership12 extends GrouperTest {

  /**
   * 
   * @param args
   */
  public static void main(String[] args) {
    TestRunner.run(new TestMembership12("testCircularWithComposites"));
  }
  
  private static final Log LOG = GrouperUtil.getLog(TestMembership12.class);

  Date before;
  R       r;
  Group   gA;
  Group   gB;
  Group   gC;
  Group   gD;
  Group   gE;
  Group   gF;

  Field fieldMembers;
  Field fieldUpdaters;

  /**
   * @param name
   */
  public TestMembership12(String name) {
    super(name);
  }

  /**
   * 
   */
  public void testCircularWithComposites() {
    LOG.info("testCircularWithComposites");
    try {
      runCompositeMembershipChangeLogConsumer();

      //sleep so if auto added members in config check, doesnt mess things up here
      GrouperUtil.sleep(50);
//      before   = DateHelper.getPastDate();
      before   = new Date();

      r     = R.populateRegistry(1, 14, 3);
      gA    = r.getGroup("a", "a");
      gB    = r.getGroup("a", "b");
      gC    = r.getGroup("a", "c");
      gD    = r.getGroup("a", "d");
      gE    = r.getGroup("a", "e");
      gF    = r.getGroup("a", "f");
      
      Subject subjA = r.getSubject("a");
      Subject subjB = r.getSubject("b");
      Subject subjC = r.getSubject("c");
      
      gB.addMember(subjA);
      gC.addMember(subjB);
      gD.addMember(subjC);

      fieldMembers = Group.getDefaultList();
      fieldUpdaters = FieldFinder.find(Field.FIELD_NAME_UPDATERS, true);

      // Test 1
      gA.addCompositeMember(CompositeType.UNION, gB, gC);
      
      try {
        gB.addMember(gA.toSubject());
        fail("Didn't throw exception");
      } catch (Exception e) {
        // good
      }
      
      
      // Test 2

      try {
        gC.addMember(gA.toSubject());
        fail("Didn't throw exception");
      } catch (Exception e) {
        // good
      }
      
      // Test 3
      gB.addMember(gD.toSubject());
      
      try {
        gD.addMember(gA.toSubject());
        fail("Didn't throw exception");
      } catch (Exception e) {
        // good
      }
      
      gB.deleteMember(gD.toSubject());
      
      // Test 4
      gD.addMember(gA.toSubject());
      
      try {
        gB.addMember(gD.toSubject());
        fail("Didn't throw exception");
      } catch (Exception e) {
        // good
      }
      
      gD.deleteMember(gA.toSubject());

      
      // Test 5
      gB.addMember(gD.toSubject());
      gD.addMember(gE.toSubject());
      gE.addMember(gF.toSubject());
      
      try {
        gF.addMember(gA.toSubject());
        fail("Didn't throw exception");
      } catch (Exception e) {
        // good
      }
      
      // undo
      gE.deleteMember(gF.toSubject());
      runCompositeMembershipChangeLogConsumer();
      T.amount("Number of list memberships", 10, MembershipFinder.internal_findAllByCreatedAfter(r.rs, before, fieldMembers).size());

      
      
      // Test 6
      gF.addMember(gA.toSubject());
      try {
        gE.addMember(gF.toSubject());
        fail("Didn't throw exception");
      } catch (Exception e) {
        // good
      }


      T.amount("Number of list memberships", 14, MembershipFinder.internal_findAllByCreatedAfter(r.rs, before, fieldMembers).size());

      
      
      // Test 7
      gA.deleteCompositeMember();
      gE.addMember(gF.toSubject());
      
      try {
        gA.addCompositeMember(CompositeType.UNION, gB, gC);
        fail("Didn't throw exception");
      } catch (Exception e) {
        // good
      }
      
      runCompositeMembershipChangeLogConsumer();
      T.amount("Number of list memberships", 14, MembershipFinder.internal_findAllByCreatedAfter(r.rs, before, fieldMembers).size());

      
      
      // privileges should still work
      gE.deleteMember(gF.toSubject());
      gA.addCompositeMember(CompositeType.UNION, gB, gC);
      gB.grantPriv(gA.toSubject(), AccessPrivilege.UPDATE);
      gD.grantPriv(gA.toSubject(), AccessPrivilege.UPDATE);
      gE.grantPriv(gA.toSubject(), AccessPrivilege.UPDATE);
      runCompositeMembershipChangeLogConsumer();

      T.amount("Number of list memberships", 14, MembershipFinder.internal_findAllByCreatedAfter(r.rs, before, fieldMembers).size());
      T.amount("Number of update privileges", 12, MembershipFinder.internal_findAllByCreatedAfter(r.rs, before, fieldUpdaters).size());
      
      r.rs.stop();
    }
    catch (Exception e) {
      T.e(e);
    }
  }
}


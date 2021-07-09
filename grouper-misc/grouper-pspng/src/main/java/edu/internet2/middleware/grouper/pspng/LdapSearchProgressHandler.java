package edu.internet2.middleware.grouper.pspng;

import org.ldaptive.LdapEntry;
import org.ldaptive.handler.LdapEntryHandler;
import org.slf4j.Logger;

public class LdapSearchProgressHandler implements LdapEntryHandler {
    ProgressMonitor progressMonitor;

    public LdapSearchProgressHandler(int numberOfExpectedResults, Logger LOG, String progressMonitorLabel) {
      progressMonitor = new ProgressMonitor(numberOfExpectedResults, LOG, true, 15, progressMonitorLabel);
    }

    public LdapSearchProgressHandler(Logger LOG, String progressMonitorLabel) {
      this(-1, LOG, progressMonitorLabel);
    }

    @Override
    public LdapEntry apply(LdapEntry ldapEntry) {
      progressMonitor.workCompleted(1);
      return ldapEntry;
    }
}

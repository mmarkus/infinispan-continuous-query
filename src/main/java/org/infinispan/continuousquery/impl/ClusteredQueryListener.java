package org.infinispan.continuousquery.impl;

import org.infinispan.continuousquery.MatchingEntry;
import org.infinispan.continuousquery.ReplayResultSetListener;
import org.infinispan.continuousquery.ResultSetListener;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;

import java.util.Iterator;
import java.util.List;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Listener 
public class ClusteredQueryListener implements ReplayResultSetListener {

   private final ResultSetListener actual;

   private final Address myAddress;

   private final Address originated;

   private List<Address> view;

   private volatile boolean iAmActive;

   @ViewChanged
   public void viewChanged(ViewChangedEvent event) {
      view = event.getNewMembers();
      checkActive();
   }

   public ClusteredQueryListener(ResultSetListener actual, Address myAddress, Address original, List<Address> view) {
      this.actual = actual;
      this.myAddress = myAddress;
      this.view = view;
      this.originated = original;
      checkActive();
   }

   @Override
   public void replay(Iterator<MatchingEntry> existingQueryResults) {
      if (actual instanceof ReplayResultSetListener && iAmActive) {
         ((ReplayResultSetListener) actual).replay(existingQueryResults);
      }
   }

   @Override
   public void entryAdded(MatchingEntry row) {
      if (iAmActive) {
         actual.entryAdded(row);
      }
   }

   @Override
   public void entryRemoved(MatchingEntry row) {
      if (iAmActive) {
         actual.entryRemoved(row);
      }
   }

   @Override
   public void entryUpdated(MatchingEntry row) {
      if (iAmActive) {
         actual.entryUpdated(row);
      }
   }

   public void checkActive() {
      if (myAddress == null)  {
         iAmActive = true;
         return;
      }
      if (myAddress.equals(originated)) {
         iAmActive = true;
      } else if (!view.contains(originated)) {
         if (view.get(view.size() - 1).equals(myAddress)) {
            iAmActive = true;
         }
      } else {
         iAmActive = false;
      }
      System.out.println("iAmActive = " + iAmActive);
   }
}

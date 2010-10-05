package org.infinispan.continuousquery.demo;

import org.infinispan.continuousquery.MatchingEntry;
import org.infinispan.continuousquery.ReplayResultSetListener;
import org.infinispan.continuousquery.ResultSetListenerSupport;

import java.util.Iterator;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public class TraderResultSetListener extends ResultSetListenerSupport implements ReplayResultSetListener {
   @Override
   public void replay(Iterator<MatchingEntry> existingQueryResults) {
      while(existingQueryResults.hasNext()) {
         System.out.println("existingQueryResults.next() = " + existingQueryResults.next());
      }
   }

   @Override
   public void entryAdded(MatchingEntry row) {
      System.out.println("row = " + row);
   }
}

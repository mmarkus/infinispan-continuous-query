package org.infinispan.continuousquery;

import java.util.Iterator;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public interface ReplayResultSetListener extends ResultSetListener {

   void replay(Iterator<MatchingEntry> existingQueryResults);
}

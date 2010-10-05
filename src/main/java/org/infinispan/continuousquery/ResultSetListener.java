package org.infinispan.continuousquery;

import java.io.Serializable;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public interface ResultSetListener extends Serializable {

   public void entryAdded(MatchingEntry row);

   public void entryRemoved(MatchingEntry row);

   public void entryUpdated(MatchingEntry row);
}

package org.infinispan.continuousquery;

import java.util.List;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public interface ContinuousQuery {

   //todo - should we move local to query definition rather???
   void addQueryListener(ResultSetListener queryListener);

   boolean removeQueryListener(ResultSetListener queryListener);

   void clearListeners();

   List<ResultSetListener> getQueryListeners();

   List<MatchingEntry> getQueryResult();

   boolean contains(Object obj, String output); 

   void close();
}

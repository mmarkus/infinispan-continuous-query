package org.infinispan.continuousquery.impl;

import org.drools.runtime.rule.LiveQuery;
import org.drools.runtime.rule.Row;
import org.drools.runtime.rule.ViewChangedEventListener;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.continuousquery.ContinuousQuery;
import org.infinispan.continuousquery.MatchingEntry;
import org.infinispan.continuousquery.QueryDefinition;
import org.infinispan.continuousquery.ResultSetListener;
import org.infinispan.continuousquery.ReplayResultSetListener;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public class ContinuousQueryImpl implements ContinuousQuery, ViewChangedEventListener {

   private final CopyOnWriteArrayList<ResultSetListener> listeners = new CopyOnWriteArrayList<ResultSetListener>();

   private final QueryDefinition queryDefinition;

   private final ContinuousQueryInterceptor cqInterceptor;

   //todo this might be written to accessed from multiple threads
   private volatile List<MatchingEntry> contQueryResult = new ArrayList<MatchingEntry>();

   private volatile LiveQuery droolsQuery;
   private Object[] params;

   private final boolean isLocal;

   private AdvancedCache cache;

   public ContinuousQueryImpl(QueryDefinition queryDefinition, ContinuousQueryInterceptor cqInterceptor, Object[] params, boolean isLocal, Cache cache) {
      this.queryDefinition = queryDefinition;
      this.cqInterceptor = cqInterceptor;
      this.params = params;
      this.isLocal = isLocal;
      this.cache = cache.getAdvancedCache();
   }

   @Override
   public void rowAdded(Row row) {
      MatchingEntryImpl matchingEntry = new MatchingEntryImpl(row, queryDefinition.getOutput());
      contQueryResult.add(matchingEntry);
      for (ResultSetListener listener : listeners) {
         listener.entryAdded(matchingEntry);
      }
   }

   @Override
   public void rowRemoved(Row row) {
      MatchingEntryImpl matchingEntry = new MatchingEntryImpl(row, queryDefinition.getOutput());
      contQueryResult.remove(matchingEntry);
      for (ResultSetListener listener : listeners) {
         listener.entryRemoved(matchingEntry);
      }
   }

   @Override
   public void rowUpdated(Row row) {
      MatchingEntryImpl matchingEntry = new MatchingEntryImpl(row, queryDefinition.getOutput());
      contQueryResult.remove(matchingEntry);
      contQueryResult.add(matchingEntry);
      for (ResultSetListener listener : listeners) {
         listener.entryUpdated(matchingEntry);
      }
   }


   @Override
   public void addQueryListener(ResultSetListener queryListener) {
      if (droolsQuery == null) throw new IllegalStateException("Cannot attach listener to a closed query!");
      if (isLocal) {
         replyIfNeeded(queryListener);
         listeners.add(queryListener);
      } else {
         cqInterceptor.replicateListener(queryListener, this);
         addClusteredQueryListener(queryListener, getLocalAddress());
      }
   }

   private void replyIfNeeded(ResultSetListener queryListener) {
      if (queryListener instanceof ReplayResultSetListener) {
         //todo this might miss/retransmit some events!!
         ((ReplayResultSetListener) queryListener).replay(contQueryResult.iterator());
      }
   }

   private Address getLocalAddress() {
      if (cache.getRpcManager() != null) return cache.getRpcManager().getAddress();
      return null;
   }

   public void addClusteredQueryListener(ResultSetListener queryListener, Address origin) {
      List<Address> view = cache.getRpcManager() != null ? cache.getRpcManager().getTransport().getMembers() : null;
      ClusteredQueryListener cqListener = new ClusteredQueryListener(queryListener, getLocalAddress(), origin, view);
      EmbeddedCacheManager ecm = (EmbeddedCacheManager) cache.getCacheManager();//todo make sure this gets removed
      ecm.addListener(cqListener);
      listeners.add(cqListener);
      replyIfNeeded(queryListener);
   }

   @Override
   public boolean removeQueryListener(ResultSetListener queryListener) {
      return listeners.remove(queryListener);
   }

   @Override
   public void clearListeners() {
      listeners.clear();
   }

   @Override
   public List<ResultSetListener> getQueryListeners() {
      return Collections.unmodifiableList(listeners);
   }

   @Override
   public List<MatchingEntry> getQueryResult() {
      return Collections.unmodifiableList(contQueryResult);
   }

   @Override
   public boolean contains(Object obj, String output) {
      for (MatchingEntry matchingEntry : contQueryResult) {
         Object val = matchingEntry.get(output);
         if (val == obj || (val != null && val.equals(obj))) return true;
      }
      return false;
   }

   @Override
   public void close() {
      droolsQuery.close();
      droolsQuery = null;
      cqInterceptor.queryClosed(this);
   }

   public void setDroolsQuery(LiveQuery droolsQuery) {
      this.droolsQuery = droolsQuery;
   }

   public String getQueryName() {
      return queryDefinition.getQueryName();
   }

   public Object[] getParams() {
      return params;
   }
}

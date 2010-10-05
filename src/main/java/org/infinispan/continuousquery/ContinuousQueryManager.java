package org.infinispan.continuousquery;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.continuousquery.impl.ContinuousQueryInterceptor;
import org.infinispan.continuousquery.impl.QueryDefinitionXmlParser;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.InvocationContextInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public class ContinuousQueryManager {

   private static Log log = LogFactory.getLog(ContinuousQueryManager.class);

   private final EmbeddedCacheManager ecm;

   public ContinuousQueryManager(EmbeddedCacheManager cacheManager, boolean includeDefaultCache, String... cacheNames) {
      this.ecm = cacheManager;
      if (includeDefaultCache) {
         prepareCache(getCache(null));
      }
      for (String cacheName : cacheNames) {
         prepareCache(getCache(cacheName));
      }
   }

   public ContinuousQueryManager(EmbeddedCacheManager cacheManager) {
      this(cacheManager, true);
   }

   private void prepareCache(Cache cache) {
      InterceptorChain chain = getInterceptorChain(cache);
      ContinuousQueryInterceptor interceptor;
      if (!chain.containsInterceptorType(ContinuousQueryInterceptor.class)) {
         interceptor = new ContinuousQueryInterceptor();
         chain.addInterceptorAfter(interceptor, InvocationContextInterceptor.class);
         cache.getAdvancedCache().getComponentRegistry().wireDependencies(interceptor);
         log.trace("ContinuousQueryInterceptor added. Chain is: " + cache.getAdvancedCache().getInterceptorChain());
      } else {
         throw new IllegalStateException("Cache already prepared!");
      }
   }

   public void defineQuery(QueryDefinition qDef) {
      String name = qDef.getCacheName();
      Cache c = getCache(name);
      getCqInterceptor(getInterceptorChain(c)).defineContinuousQuery(qDef);
   }

   public void defineQueries(String fileName) {
      List<QueryDefinition> queries = new QueryDefinitionXmlParser().parse(fileName);
      for (QueryDefinition q : queries) {
         defineQuery(q);
      }
   }

   public ContinuousQuery executeContinuousQuery(String cacheName, String queryName, boolean local, Object... params) {
      ContinuousQueryInterceptor cqi = getCqInterceptor(getInterceptorChain(getCache(cacheName)));
      return cqi.executeContinuousQuery(queryName, params, local);
   }

   public ContinuousQuery executeContinuousQuery(String queryName, boolean local, Object... params) {
      return executeContinuousQuery(null, queryName, local, params);
   }

   public ContinuousQuery getContinuousQuery(String queryName) {
     return getCqInterceptor(getInterceptorChain(getCache(null))).getContinuousQuery(queryName);
   }

   private Cache getCache(String cacheName) {
      if (cacheName == null) return ecm.getCache();
      else return ecm.getCache(cacheName);
   }

   private InterceptorChain getInterceptorChain(Cache c) {
      AdvancedCache ac = c.getAdvancedCache();
      return ac.getComponentRegistry().getComponent(InterceptorChain.class);
   }

   private ContinuousQueryInterceptor getCqInterceptor(InterceptorChain chain) {
      List<CommandInterceptor> interceptors = chain.getInterceptorsWithClassName(ContinuousQueryInterceptor.class.getName());
      if (!(interceptors.size() == 1)) {
         throw new IllegalStateException("Expected only one interceptor but got: " + interceptors.size());
      }
      return (ContinuousQueryInterceptor) interceptors.get(0);
   }

   void resetData() {
      getCqInterceptor(getInterceptorChain(getCache(null))).resetQueryData();
   }

   public Set<QueryDefinition> getDefinedQueries() {
      return getCqInterceptor(getInterceptorChain(getCache(null))).getDefinedQueries();
   }

   public Set<QueryDefinition> getDefinedQueries(String cacheName) {
      return getCqInterceptor(getInterceptorChain(getCache(cacheName))).getDefinedQueries();
   }

   public Object getDefinedQuery(String qName) {
      for (QueryDefinition queryDefinition : getDefinedQueries()) {
         if (queryDefinition.getQueryName().equals(qName)) return queryDefinition;
      }
      return null;
   }

   public Object getDefinedQuery(String cacheName, String qName) {
      for (QueryDefinition queryDefinition : getDefinedQueries(cacheName)) {
         if (queryDefinition.getQueryName().equals(qName)) return queryDefinition; 
      }
      return null;
   }
}

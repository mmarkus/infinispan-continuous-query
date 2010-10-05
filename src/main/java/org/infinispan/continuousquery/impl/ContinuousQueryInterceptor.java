package org.infinispan.continuousquery.impl;

import net.jcip.annotations.GuardedBy;
import org.drools.KnowledgeBase;
import org.drools.KnowledgeBaseFactory;
import org.drools.builder.KnowledgeBuilder;
import org.drools.builder.KnowledgeBuilderFactory;
import org.drools.builder.ResourceType;
import org.drools.io.ResourceFactory;
import org.drools.runtime.StatefulKnowledgeSession;
import org.drools.runtime.rule.FactHandle;
import org.drools.runtime.rule.LiveQuery;
import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.continuousquery.ContinuousQuery;
import org.infinispan.continuousquery.QueryDefinition;
import org.infinispan.continuousquery.ResultSetListener;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * // TODO - part of this logic should be moved in an CQManager as this class is doing "too much" for an interceptor
 * // TODO - test in detail cluster join and leaves especially for clustreded queries
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public class ContinuousQueryInterceptor extends CommandInterceptor {

   private static Log log = LogFactory.getLog(ContinuousQueryInterceptor.class);

   private final KnowledgeBase kBase = KnowledgeBaseFactory.newKnowledgeBase();
   private volatile StatefulKnowledgeSession kSession = kBase.newStatefulKnowledgeSession();

   private static final String DEFINE_REMOTE_QUERY = "___define_remote_query__";
   private static final String REPLICATE_QUERY_LISTENER = "___replicate_query_listener__";
   private static final String REPLICATE_QUERY = "___replicate_query__";


   private RpcManager rpcManager;

   private CommandsFactory commandsFactory;

   //this can be avoided by wrapping the values in some structure
   private Map<Object, FactHandle> factHandles = new ConcurrentHashMap<Object, FactHandle>();

   @GuardedBy("cqs")
   //writes to this are guarded to make sure that two nodes aren't writing at the same time
   private ConcurrentHashMap<String, ContinuousQuery> cqs = new ConcurrentHashMap();

   private final ConcurrentHashMap<String, QueryDefinition> defs = new ConcurrentHashMap<String, QueryDefinition>();
   private Cache cache;

   @Inject
   public void init(RpcManager rpcManager, CommandsFactory commandsFactory, Cache cache) {
      this.rpcManager = rpcManager;
      this.commandsFactory = commandsFactory;
      this.cache = cache;
   }

   public void defineContinuousQuery(QueryDefinition qDef) {
      defineQueryLocally(qDef);
      if (rpcManager != null) {
         PutKeyValueCommand command = commandsFactory.buildPutKeyValueCommand(DEFINE_REMOTE_QUERY, qDef, 0, 0);
         rpcManager.broadcastRpcCommand(command, true);
      }
   }

   private void defineQueryLocally(QueryDefinition qDef) {
      KnowledgeBuilder kbuilder = KnowledgeBuilderFactory.newKnowledgeBuilder();
      kbuilder.add(ResourceFactory.newByteArrayResource(qDef.getQueryStr().getBytes()), ResourceType.DRL);
      if (kbuilder.hasErrors()) {
         throw new IllegalStateException("Invalid query:" + kbuilder.getErrors().toString());
      }
      defs.put(qDef.getQueryName(), qDef);
      if (log.isTraceEnabled()) log.trace("Available definitions are: " + defs.keySet());
      //todo cofirm with mark p.
      kBase.addKnowledgePackages(kbuilder.getKnowledgePackages());
   }

   public ContinuousQuery executeContinuousQuery(String queryName, Object[] params, boolean local) {
      synchronized (cqs) {
         executeQuery(queryName, local, params);
         if (!local && rpcManager != null) {
            PutKeyValueCommand command = commandsFactory.buildPutKeyValueCommand(REPLICATE_QUERY, new Object[]{queryName, params}, 0, 0);
            rpcManager.broadcastRpcCommand(command, true);
         }
         if (defs.get(queryName).getDefaultResultSetListener() != null) {
            cqs.get(queryName).addQueryListener(newInstance(defs.get(queryName).getDefaultResultSetListener()));
         }
      }
      return cqs.get(queryName);
   }

   private void executeQuery(String queryName, boolean isLocal, Object[] params) {
      if (!defs.containsKey(queryName)) {
         String message = "Query '" + queryName + "' has not been defined! Available defs are: " + defs.keySet();
         log.error(message);
         throw new IllegalStateException(message);
      }
      QueryDefinition definition = defs.get(queryName);
      ContinuousQueryImpl cq = new ContinuousQueryImpl(definition, this, params, isLocal, cache);

      //todo cqs should rather contain exec instances that simple queryName. A query can be executed several times with different execution instance
      cqs.put(queryName, cq);
      LiveQuery liveQuery = kSession.openLiveQuery(queryName, params, cq);
      cq.setDroolsQuery(liveQuery);
   }

   private ResultSetListener newInstance(String defaultResultSetListener) {
      try {
         return (ResultSetListener) Class.forName(defaultResultSetListener).newInstance();
      } catch (Exception e) {
         log.error("Unexpected!", e);
         throw new CacheException(e);
      }
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {

      if (DEFINE_REMOTE_QUERY.equals(command.getKey())) {
         if (log.isTraceEnabled()) log.trace("received cq " + command);
         defineQueryLocally((QueryDefinition) command.getValue());
         return null;
      }

      if (REPLICATE_QUERY.equals(command.getKey())) {
         Object[] vals = (Object[]) command.getValue();
         executeQuery((String) vals[0], false, (Object[]) vals[1]);
         return null;
      }

      if (REPLICATE_QUERY_LISTENER.equals(command.getKey())) {
         if (log.isTraceEnabled()) log.trace("received cq listener" + command);
         Object[] vals = (Object[]) command.getValue();
         ResultSetListener ql = (ResultSetListener) vals[0];
         String qName = (String) vals[1];
         Address origin = (Address) vals[2];
         ContinuousQueryImpl cq = (ContinuousQueryImpl) cqs.get(qName);
         if (cq == null) throw new IllegalStateException("Could not find query named: " + qName);
         cq.addClusteredQueryListener(ql, origin);
         return null;
      }

      //todo - optimization - only add it if the value is part of an existing query imports. Can we obtain that from drools
      //todo otherwise we should explicitly expose some filters here
      FactHandle factHandle = factHandles.get(command.getKey());

      //todo replace retract with update (update doesn't seem to work correctly in drools) if possible
      if (factHandle != null) {
         kSession.retract(factHandle);
      }
      FactHandle factHandle1 = kSession.insert(command.getValue());
      factHandles.put(command.getKey(), factHandle1);
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      FactHandle factHandle = factHandles.remove(command.getKey());
      if (factHandle != null) kSession.retract(factHandle);
      return invokeNextInterceptor(ctx, command);
   }


   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      for (FactHandle fh : factHandles.values()) {
         kSession.retract(fh);
      }
      factHandles.clear();
      return invokeNextInterceptor(ctx, command);
   }

   public void queryClosed(ContinuousQueryImpl continuousQuery) {
      cqs.remove(continuousQuery.getQueryName());
   }

   public void resetQueryData() {
      kSession.dispose();
      kSession = kBase.newStatefulKnowledgeSession();
      factHandles.clear();
      cqs.clear();
   }

   public Set<QueryDefinition> getDefinedQueries() {
      return Collections.unmodifiableSet(new HashSet(defs.values()));
   }

   public ContinuousQuery getContinuousQuery(String queryName) {
      return cqs.get(queryName);
   }

   public void replicateListener(ResultSetListener queryListener, ContinuousQueryImpl continuousQuery) {
      if (rpcManager == null) return;
      Address origin = rpcManager.getAddress();
      Object[] value = {queryListener, continuousQuery.getQueryName(), origin};
      PutKeyValueCommand command = commandsFactory.buildPutKeyValueCommand(REPLICATE_QUERY_LISTENER, value, 0, 0);
      rpcManager.broadcastRpcCommand(command, true);
   }
}

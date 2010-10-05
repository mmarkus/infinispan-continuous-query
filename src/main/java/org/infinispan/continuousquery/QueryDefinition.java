package org.infinispan.continuousquery;

import java.io.Serializable;
import java.util.List;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public class QueryDefinition implements Serializable {

   private String queryName;

   private String queryStr;

   private String cacheName;

   private String defaultResultSetListener;

   private final List<String> output;

   public QueryDefinition(String queryName, String queryStr, String cacheName, List<String> output, String defaultResultSetListener) {
      this.queryName = queryName;
      this.queryStr = queryStr;
      this.cacheName = cacheName;
      this.output = output;
      this.defaultResultSetListener = defaultResultSetListener;
   }

   public QueryDefinition(String queryName, String queryStr, List<String> output, String defaultResultSetListener) {
      this(queryName, queryStr, null, output, defaultResultSetListener);
   }

   public QueryDefinition(String queryName, String queryStr, List<String> output) {
      this(queryName, queryStr, null, output, null);
   }

   public String getQueryName() {
      return queryName;
   }

   public String getQueryStr() {
      return queryStr;
   }

   public String getCacheName() {
      return cacheName;
   }

   public List<String> getOutput() {
      return output;
   }

   public String getDefaultResultSetListener() {
      return defaultResultSetListener;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      QueryDefinition that = (QueryDefinition) o;

      if (cacheName != null ? !cacheName.equals(that.cacheName) : that.cacheName != null) return false;
      if (output != null ? !output.equals(that.output) : that.output != null) return false;
      if (queryName != null ? !queryName.equals(that.queryName) : that.queryName != null) return false;
      if (queryStr != null ? !queryStr.equals(that.queryStr) : that.queryStr != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = queryName != null ? queryName.hashCode() : 0;
      result = 31 * result + (queryStr != null ? queryStr.hashCode() : 0);
      result = 31 * result + (cacheName != null ? cacheName.hashCode() : 0);
      result = 31 * result + (output != null ? output.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "QueryDefinition{" +
            "queryName='" + queryName + '\'' +
            ", queryStr='" + queryStr + '\'' +
            ", cacheName='" + cacheName + '\'' +
            ", output=" + output +
            '}';
   }
}

package org.infinispan.continuousquery.impl;

import net.jcip.annotations.Immutable;
import org.drools.runtime.rule.Row;
import org.infinispan.continuousquery.MatchingEntry;

import java.util.List;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Immutable
public class MatchingEntryImpl implements MatchingEntry {

   private final Row row;

   private final List<String> output;

   public MatchingEntryImpl(Row row, List<String> output) {
      this.row = row;
      this.output = output;
   }

   @Override
   public Object get(String colName) {
      return row.get(colName);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      MatchingEntryImpl matchingEntry = (MatchingEntryImpl) o;
      for (String col : output) {
         Object otherValue = matchingEntry.get(col);
         Object ourValue = this.get(col);
         if (! ((otherValue == ourValue) || (otherValue != null && otherValue.equals(ourValue)))) return false;
      }
      return true;
   }

   @Override
   public int hashCode() {
      int result = row != null ? row.hashCode() : 0;
      result = 31 * result + (output != null ? output.hashCode() : 0);
      return result;
   }
}

package org.infinispan.continuousquery.demo;

import java.io.Serializable;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public class Trader implements Serializable {
   private String name;

   public Trader(String name) {
      this.name = name;
   }

   public String getName() {
      return name;
   }

   @Override
   public String toString() {
      return "Trader{" +
            "name='" + name + '\'' +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Trader trader = (Trader) o;

      if (name != null ? !name.equals(trader.name) : trader.name != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return name != null ? name.hashCode() : 0;
   }
}

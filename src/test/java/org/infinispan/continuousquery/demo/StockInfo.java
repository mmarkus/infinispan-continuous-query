package org.infinispan.continuousquery.demo;

import java.io.Serializable;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public class StockInfo implements Serializable {

   private String company;

   private double stockUnitValue;

   private String profile;

   public StockInfo(String company, double stockUnitValue, String profile) {
      this.company = company;
      this.stockUnitValue = stockUnitValue;
      this.profile = profile;
   }

   public String getCompany() {
      return company;
   }

   public double getStockUnitValue() {
      return stockUnitValue;
   }

   public String getProfile() {
      return profile;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      StockInfo stockInfo = (StockInfo) o;

      if (Double.compare(stockInfo.stockUnitValue, stockUnitValue) != 0) return false;
      if (company != null ? !company.equals(stockInfo.company) : stockInfo.company != null) return false;
      if (profile != null ? !profile.equals(stockInfo.profile) : stockInfo.profile != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result;
      long temp;
      result = company != null ? company.hashCode() : 0;
      temp = stockUnitValue != +0.0d ? Double.doubleToLongBits(stockUnitValue) : 0L;
      result = 31 * result + (int) (temp ^ (temp >>> 32));
      result = 31 * result + (profile != null ? profile.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "StockInfo{" +
            "company='" + company + '\'' +
            ", stockUnitValue=" + stockUnitValue +
            ", profile='" + profile + '\'' +
            '}';
   }
}

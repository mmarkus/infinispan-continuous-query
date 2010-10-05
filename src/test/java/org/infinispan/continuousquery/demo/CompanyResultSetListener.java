package org.infinispan.continuousquery.demo;

import org.infinispan.continuousquery.MatchingEntry;
import org.infinispan.continuousquery.ReplayResultSetListener;
import org.infinispan.continuousquery.ResultSetListenerSupport;

import java.util.Iterator;

public class CompanyResultSetListener extends ResultSetListenerSupport implements ReplayResultSetListener {

   @Override
   public void replay(Iterator<MatchingEntry> existingQueryResults) {
      String companies = "";
      while (existingQueryResults.hasNext()) {
         Object si = existingQueryResults.next().get("stockInfo");
         companies += ("  " + si);
      }
      if (companies.length() > 0) {
         System.out.println("Companies that match your profile are: " + companies);
      } else {
         System.out.println("No company matches your profile!");
      }
   }

   @Override
   public void entryAdded(MatchingEntry entry) {
      Object company = entry.get("stockInfo");
      System.out.println("Hey! A company that matches your profile is open for trading: " + company);
   }
}
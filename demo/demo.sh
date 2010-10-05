#!/bin/bash

java -cp ./target/continuous-query-jar-with-dependencies.jar:./target/test-classes -Djava.net.preferIPv4Stack=true -Djgroups.bind_addr=127.0.0.1 -Dlog4j.configuration=log4j2.xml org.infinispan.continuousquery.demo.StockExampleDemo $1 $2
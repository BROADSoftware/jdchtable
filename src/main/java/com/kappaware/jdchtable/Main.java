package com.kappaware.jdchtable;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Main {
	static Logger log = LoggerFactory.getLogger(Main.class);

	static public void main(String[] argv) throws Exception {
		log.info("jdchtable start");
		
	    
	       // The following will remove the message: 2014-06-14 01:38:59.359 java[993:1903] Unable to load realm info from SCDynamicStore
	       // Equivalent to HADOOP_OPTS="${HADOOP_OPTS} -Djava.security.krb5.conf=/dev/null"
	       // Of course, should be configured properly in case of use of Kerberos
	       //System.setProperty("java.security.krb5.conf", "/dev/null");
	   
	       Configuration config = HBaseConfiguration.create();
	       config.set("hbase.zookeeper.quorum", "sr1.hdp2.bsa.broadsoftware.com,nn1.hdp2.bsa.broadsoftware.com,nn2.hdp2.bsa.broadsoftware.com");
	       config.set("zookeeper.znode.parent", "/hbase-unsecure");

	       try {
	    	   Connection connection = ConnectionFactory.createConnection(config);
	    	   Admin hbAdmin = connection.getAdmin();
	           HTableDescriptor[] tda = hbAdmin.listTables();
	           for (HTableDescriptor hdesc : tda) {
	               System.out.print(String.format("Table: %s\n", hdesc.getNameAsString()));
	           }
	           hbAdmin.close();
	       } catch (IOException e) {
	           e.printStackTrace();
	       }

		
	}

}

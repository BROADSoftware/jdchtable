package com.kappaware.jdchtable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;
import com.kappaware.jdchtable.config.JdcConfiguration;
import com.kappaware.jdchtable.HDescription.Namespace;
import com.kappaware.jdchtable.HDescription.State;
import com.kappaware.jdchtable.config.ConfigurationException;
import com.kappaware.jdchtable.config.JdcConfigurationImpl;
import com.kappaware.jdchtable.config.Parameters;

public class Main {
	static Logger log = LoggerFactory.getLogger(Main.class);

	static public void main(String[] argv) throws IOException  {
		try {
			main2(argv);
		} catch(ConfigurationException | DescriptionException | FileNotFoundException | YamlException  e) {
			log.error(e.getMessage());
			System.err.println("ERROR: " + e.getMessage());
		}
	}
	
	
	
	static public void main2(String[] argv) throws ConfigurationException, DescriptionException, IOException {
		log.info("jdchtable start");

		JdcConfiguration jdcConfiguration = new JdcConfigurationImpl(new Parameters(argv));

		File file = new File(jdcConfiguration.getInputFile());

		if (!file.canRead()) {
			throw new ConfigurationException(String.format("Unable to open '%s' for reading", file.getAbsolutePath()));
		}
		YamlReader yamlReader = new YamlReader(new FileReader(file), HDescription.getYamlConfig());
		HDescription hDescription = yamlReader.read(HDescription.class);
		if (jdcConfiguration.getZookeeper() != null) {
			hDescription.zookeeper = jdcConfiguration.getZookeeper(); // Override the value in file
		}
		if (jdcConfiguration.getZnodeParent() != null) {
			hDescription.znodeParent = jdcConfiguration.getZnodeParent(); // Override the value in file
		}

		// The following will remove the message: 2014-06-14 01:38:59.359 java[993:1903] Unable to load realm info from SCDynamicStore
		// Equivalent to HADOOP_OPTS="${HADOOP_OPTS} -Djava.security.krb5.conf=/dev/null"
		// Of course, should be configured properly in case of use of Kerberos
		//System.setProperty("java.security.krb5.conf", "/dev/null");

		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", hDescription.zookeeper);
		if(hDescription.znodeParent != null) {
			config.set("zookeeper.znode.parent", hDescription.znodeParent);
		}

		int nbrModif = 0;
		Admin hbAdmin = null;
		try {
			Connection connection = ConnectionFactory.createConnection(config);
			hbAdmin = connection.getAdmin();
			
			Map<String, NamespaceDescriptor> nsDescByName = new HashMap<String, NamespaceDescriptor>();
			Set<String> namespaceToDelete = new HashSet<String>();
			NamespaceDescriptor[] namespaceDescriptors = hbAdmin.listNamespaceDescriptors();
			for (NamespaceDescriptor ndesc : namespaceDescriptors) {
				nsDescByName.put(ndesc.getName(), ndesc);
				//System.out.print(String.format("Namespace: %s\n", ndesc.getName()));
			}
			// A loop in namespace to:
			// - Create namespace when needed
			// - Mark namespace to delete
			if( hDescription.namespaces == null) {
				throw new DescriptionException("Invalid description: namespaces list must be defined ");
			}
			for(Namespace ns : hDescription.namespaces) {
				if(ns.name == null) {
					throw new DescriptionException("Invalid description: Every namespace must have a 'name' attribute");
				}
				if(ns.state != null && ns.state == State.absent) {
					if(nsDescByName.containsKey(ns.name)) {
						namespaceToDelete.add(ns.name);
					}
				} else {
					if(!nsDescByName.containsKey(ns.name)) {
						log.info(String.format("Will create namespace '%s'", ns.name));
						nbrModif++;
						hbAdmin.createNamespace(NamespaceDescriptor.create(ns.name).build());
					}
				}
			}
			
			

			// Delete namespace marked for deletion
			for(String nspace : namespaceToDelete) {
				log.info(String.format("Will delete namespace '%s'", nspace));
				nbrModif++;
				hbAdmin.deleteNamespace(nspace);
			}
			
			
			HTableDescriptor[] tda = hbAdmin.listTables();
			for (HTableDescriptor hdesc : tda) {
				System.out.print(String.format("Table: %s\n", hdesc.getNameAsString()));
			}
			hbAdmin.close();
			System.out.println(String.format("jdchtable: %s modification(s)", nbrModif));
		} finally {
			if(hbAdmin != null) {
				hbAdmin.close();
			}
		}
	}

}

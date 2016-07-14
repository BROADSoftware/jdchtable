/*
 * Copyright (C) 2016 BROADSoftware
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.kappaware.jdchtable;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.yamlbeans.YamlException;
import com.esotericsoftware.yamlbeans.YamlReader;
import com.kappaware.jdchtable.config.JdcConfiguration;
import com.kappaware.jdchtable.config.ConfigurationException;
import com.kappaware.jdchtable.config.JdcConfigurationImpl;
import com.kappaware.jdchtable.config.Parameters;

public class Main {
	static Logger log = LoggerFactory.getLogger(Main.class);
	

	static public void main(String[] argv) throws IOException {
		try {
			main2(argv);
			System.exit(0);
		} catch (ConfigurationException | DescriptionException | FileNotFoundException | YamlException | InterruptedException e) {
			//log.error(e.getMessage());
			System.err.println("ERROR: " + e.getMessage());
			System.exit(1);
		}
	}

	static public void main2(String[] argv) throws ConfigurationException, DescriptionException, IOException, InterruptedException {
		log.info("jdchtable start");

		JdcConfiguration jdcConfiguration = new JdcConfigurationImpl(new Parameters(argv));

		File file = new File(jdcConfiguration.getInputFile());

		if (!file.canRead()) {
			throw new ConfigurationException(String.format("Unable to open '%s' for reading", file.getAbsolutePath()));
		}
		YamlReader yamlReader = new YamlReader(new FileReader(file), Description.getYamlConfig());
		Description description = yamlReader.read(Description.class);
		if (jdcConfiguration.getZookeeper() != null) {
			description.zookeeper = jdcConfiguration.getZookeeper(); // Override the value in file
		}
		if (jdcConfiguration.getZnodeParent() != null) {
			description.znodeParent = jdcConfiguration.getZnodeParent(); // Override the value in file
		}
		description.polish(jdcConfiguration.getDefaultState());
		
		// The following will remove the message: 2014-06-14 01:38:59.359 java[993:1903] Unable to load realm info from SCDynamicStore
		// Equivalent to HADOOP_OPTS="${HADOOP_OPTS} -Djava.security.krb5.conf=/dev/null"
		// Of course, should be configured properly in case of use of Kerberos
		//System.setProperty("java.security.krb5.conf", "/dev/null");

		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", description.zookeeper);
		if (description.znodeParent != null) {
			config.set("zookeeper.znode.parent", description.znodeParent);
		}

		Admin hbAdmin = null;
		try {
			Connection connection = ConnectionFactory.createConnection(config);
			hbAdmin = connection.getAdmin();
			Engine engine = new Engine(hbAdmin, description);
			engine.run();
		
		} finally {
			if (hbAdmin != null) {
				hbAdmin.close();
			}
		}
	}


}

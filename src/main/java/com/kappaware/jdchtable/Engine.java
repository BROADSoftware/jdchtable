package com.kappaware.jdchtable;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kappaware.jdchtable.Description.ColumnFamily;
import com.kappaware.jdchtable.Description.Namespace;
import com.kappaware.jdchtable.Description.State;
import com.kappaware.jdchtable.Description.Table;

public class Engine {
	static Logger log = LoggerFactory.getLogger(Engine.class);

	static BeanHelper HTableDescriptorBeanHelper = new BeanHelper(HTableDescriptor.class, "table");
	static BeanHelper HColumnDescriptorBeanHelper = new BeanHelper(HColumnDescriptor.class, "columnFamily");

	private Admin hbAdmin;
	private Description description;
	private int nbrModif = 0;
	
	Engine(Admin hbAdmin, Description description) {
		this.hbAdmin = hbAdmin;
		this.description = description;
	}

	
	void run() throws IOException, DescriptionException {
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
		if (description.namespaces == null) {
			throw new DescriptionException("Invalid description: namespaces list must be defined ");
		}
		for (Namespace ns : description.namespaces) {
			if (ns.name == null) {
				throw new DescriptionException("Invalid description: Every namespace must have a 'name' attribute");
			}
			if (ns.state != null && ns.state == State.absent) {
				if (nsDescByName.containsKey(ns.name)) {
					if (Utils.DEFAULT_NS.equals(ns.name)) {
						throw new DescriptionException("Can't delete 'default' namespace");
					}
					namespaceToDelete.add(ns.name);
				}
			} else {
				if (!nsDescByName.containsKey(ns.name)) {
					log.info(String.format("Will create namespace '%s'", ns.name));
					nbrModif++;
					hbAdmin.createNamespace(NamespaceDescriptor.create(ns.name).build());
				}
			}
		}

		HTableDescriptor[] tda = hbAdmin.listTables();
		Map<TableName, HTableDescriptor> hTableDescriptorByName = new HashMap<TableName, HTableDescriptor>();
		for (HTableDescriptor hdesc : tda) {
			log.debug(String.format("Found table '%s'", hdesc.getTableName().toString()));
			hTableDescriptorByName.put(hdesc.getTableName(), hdesc);
		}
		
		
		
		for (Namespace ns : description.namespaces) {
			if (ns.tables != null) {
				for (Table table : ns.tables) {
					table.polish(ns.name);
					HTableDescriptor hdesc = hTableDescriptorByName.get(table.getName());
					if (hdesc == null) {
						if (table.getState() == State.present) {
							createTable(table);
						}
					} else {
						if (table.getState() == State.present) {
							adjustTable(hdesc, table);
						} else {
							deleteTable(table);
						}
					}
				}
			}
		}

		// Delete namespace marked for deletion
		for (String nspace : namespaceToDelete) {
			log.info(String.format("Will delete namespace '%s'", nspace));
			nbrModif++;
			hbAdmin.deleteNamespace(nspace);
		}
		System.out.println(String.format("jdchtable: %s modification(s)", nbrModif));

	}
	

	private void deleteTable(Table table) {
		// TODO Auto-generated method stub

	}

	private void adjustTable(HTableDescriptor hdesc, Table table) {
		// TODO Auto-generated method stub

	}

	private void createTable(Table table) throws DescriptionException, IOException {
		HTableDescriptor tDesc = new HTableDescriptor(table.getName());
		for (String propertyName : table.keySet()) {
			this.nbrModif += HTableDescriptorBeanHelper.set(tDesc, propertyName, table.get(propertyName));
		}
		for(ColumnFamily cf : table.getColumnFamilies()) {
			HColumnDescriptor cfDesc = new HColumnDescriptor(cf.getName());
			for (String propertyName : cf.keySet()) {
				this.nbrModif += HColumnDescriptorBeanHelper.set(cfDesc, propertyName, cf.get(propertyName));
			}
			tDesc.addFamily(cfDesc);
		}
		this.hbAdmin.createTable(tDesc);
	}
	
}

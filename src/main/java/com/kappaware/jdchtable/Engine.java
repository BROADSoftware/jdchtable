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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
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

	void run() throws IOException, DescriptionException, InterruptedException {
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

		Map<TableName, HTableDescriptor> hTableDescriptorByName = new HashMap<TableName, HTableDescriptor>();
		for (HTableDescriptor hdesc : hbAdmin.listTables()) {
			log.debug(String.format("Found table '%s'", hdesc.getTableName().toString()));
			hTableDescriptorByName.put(hdesc.getTableName(), hdesc);
		}

		for (Namespace ns : description.namespaces) {
			if (ns.tables != null) {
				Set<String> tableNames = new HashSet<String>();
				for (Table table : ns.tables) {
					if (tableNames.contains(table.name)) {
						throw new DescriptionException(String.format("Table '%s' is defined twice in namespace '%s'", table.name, ns.name));
					}
					tableNames.add(table.name);
					HTableDescriptor hdesc = hTableDescriptorByName.get(table.tableName);
					if (hdesc == null) {
						if (table.state == State.present) {
							createTable(table);
						}
					} else {
						if (table.state == State.present) {
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
			Set<String> hdl = new HashSet<String>();
			for (HTableDescriptor hdesc : hbAdmin.listTables()) {
				if (nspace.equals(hdesc.getTableName().getNamespaceAsString())) {
					hdl.add(hdesc.getTableName().getQualifierAsString());
				}
			}
			if (hdl.size() > 0) {
				throw new DescriptionException(String.format("Unable to delete namespace '%s'. There is still existing table(s): %s", nspace, hdl.toString()));
			}
			log.info(String.format("Will delete namespace '%s'", nspace));
			nbrModif++;
			hbAdmin.deleteNamespace(nspace);
		}
		String m = String.format("jdchtable: %d modification(s)", nbrModif);
		System.out.println(m);
		log.info(m);
	}

	private void deleteTable(Table table) throws IOException {
		log.info(String.format("Will delete table '%s'", table.tableName.toString()));
		if (this.hbAdmin.isTableEnabled(table.tableName)) {
			this.hbAdmin.disableTable(table.tableName);
		}
		this.nbrModif++;
		this.hbAdmin.deleteTable(table.tableName);
	}

	private void adjustTable(HTableDescriptor tDesc, Table table) throws DescriptionException, IOException, InterruptedException {
		int initialNbrModif = this.nbrModif;
		if (table.properties != null) {
			for (String propertyName : table.properties.keySet()) {
				this.nbrModif += HTableDescriptorBeanHelper.set(tDesc, propertyName, table.properties.get(propertyName));
			}
		}
		Map<String, HColumnDescriptor> hColumnDescriptorByName = new HashMap<String, HColumnDescriptor>();
		for (HColumnDescriptor cd : tDesc.getColumnFamilies()) {
			hColumnDescriptorByName.put(cd.getNameAsString(), cd);
		}
		for (ColumnFamily cf : table.columnFamilies) {
			HColumnDescriptor cd = hColumnDescriptorByName.get(cf.name);
			if (cd == null) {
				if (cf.state == State.present) {
					log.info(String.format("Will add column family %s on table %s", cf.name, table.tableName.toString()));
					tDesc.addFamily(createColumnFamily(cf));
					this.nbrModif++;
				} // Else, nothing to do
			} else {
				if (cf.state == State.present) {
					adjustColumnFamily(cd, cf);
				} else {
					log.info(String.format("Will remove column family %s of table %s", cf.name, table.tableName.toString()));
					tDesc.removeFamily(Bytes.toBytes(cf.name));
					this.nbrModif++;
				}
				hColumnDescriptorByName.remove(cf.name); // To mark as handled
			}
		}
		if (hColumnDescriptorByName.size() > 0) {
			throw new DescriptionException(String.format("Table '%s': One or several column familly exist but are not described (%s). Set state: absent if you want to remove them", table.tableName.toString(), hColumnDescriptorByName.keySet().toString()));
		}
		if (initialNbrModif != this.nbrModif) {
			boolean wasEnabled = false;
			if (this.hbAdmin.isTableEnabled(table.tableName)) {
				this.hbAdmin.disableTable(table.tableName);
				wasEnabled = true;
			}
			this.hbAdmin.modifyTable(table.tableName, tDesc);
			Pair<Integer, Integer> status = null;
			for (int i = 0; i < 500; i++) {
				status = this.hbAdmin.getAlterStatus(table.tableName);
				if ((i % 10) == 0 || status.getFirst() == 0) {
					log.info(String.format("Table '%s'. %d region(s) on %d was successfully updated", table.tableName.toString(),  status.getSecond() - status.getFirst(), status.getSecond()));
				}
				if (status.getFirst() == 0) {
					break;
				} else {
					Thread.sleep(1000);
				}
			}
			if (status.getFirst() > 0) {
				throw new DescriptionException(String.format("Table '%s': Unable to update in less than 500s. Still %d region remaining", table.tableName.toString(), status.getFirst()));
			}
			if(wasEnabled) {
				this.hbAdmin.enableTable(table.tableName);
			}
		}
	}

	private void adjustColumnFamily(HColumnDescriptor cd, ColumnFamily cf) throws DescriptionException {
		if (cf.properties != null) {
			for (String propertyName : cf.properties.keySet()) {
				this.nbrModif += HColumnDescriptorBeanHelper.set(cd, propertyName, cf.properties.get(propertyName));
			}
		}
	}

	private HColumnDescriptor createColumnFamily(ColumnFamily cf) throws DescriptionException {
		HColumnDescriptor cfDesc = new HColumnDescriptor(cf.name);
		if (cf.properties != null) {
			for (String propertyName : cf.properties.keySet()) {
				this.nbrModif += HColumnDescriptorBeanHelper.set(cfDesc, propertyName, cf.properties.get(propertyName));
			}
		}
		return cfDesc;
	}

	private void createTable(Table table) throws DescriptionException, IOException {
		HTableDescriptor tDesc = new HTableDescriptor(table.tableName);
		if (table.properties != null) {
			for (String propertyName : table.properties.keySet()) {
				this.nbrModif += HTableDescriptorBeanHelper.set(tDesc, propertyName, table.properties.get(propertyName));
			}
		}
		if (table.columnFamilies != null) {
			for (ColumnFamily cf : table.columnFamilies) {
				if (cf.state == State.present) {
					tDesc.addFamily(this.createColumnFamily(cf));
					this.nbrModif++;
				}
			}
		}
		log.info(String.format("Will create table '%s' with %d column familly(ies)", table.tableName.toString(), table.columnFamilies == null ? 0 : table.columnFamilies.size()));
		if (table.presplit != null) {
			if (table.presplit.keysAsString != null) {
				int size = table.presplit.keysAsString.size();
				byte[][] splitKeys = new byte[size][];
				for (int i = 0; i < size; i++) {
					splitKeys[i] = Bytes.toBytes(table.presplit.keysAsString.get(i));
				}
				this.hbAdmin.createTable(tDesc, splitKeys);
			} else if (table.presplit.keysAsNumber != null) {
				int size = table.presplit.keysAsNumber.size();
				byte[][] splitKeys = new byte[size][];
				for (int i = 0; i < size; i++) {
					//splitKeys[i] = Bytes.toBytes(new BigDecimal(table.presplit.keysAsNumber.get(i).toString()));
					splitKeys[i] = Bytes.toBytes(table.presplit.keysAsNumber.get(i));
				}
				this.hbAdmin.createTable(tDesc, splitKeys);
			} else {
				this.hbAdmin.createTable(tDesc, Bytes.toBytes(table.presplit.startKey), Bytes.toBytes(table.presplit.endKey), table.presplit.numRegion);
			}
		} else {
			this.hbAdmin.createTable(tDesc);
		}
		this.nbrModif++;
	}

}

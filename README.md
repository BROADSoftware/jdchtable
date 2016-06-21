# jdchtable

An idempotent tool to easily create and maintain HBase table.

When deploying an application in the Hadoop/HBase world, a common issue is to create the required table.

This is usually achieved using scripts. But, this can quickly become cumbersome and hard to maintain. And a nightmare when it come to updating a running application.

'jdc' stand for 'Just DesCribe'. You define all the namespace, table, column, properties of your HBase application in a simple YAML file and jdchtable will take care of deploying all theses object on your cluster.

In case of schema evolution, just change the description file, apply it again, and appropriate modification will be issued.

jdctable is a convergence tool. Its aim is to align the real configuration to what is described on the source file, while applying only strictly necessary modification.

This make jdchtable a fully idempotent tool, as all modern DevOps tools should be.

***
## Usage

jdchtable is provided as rpm packages (Sorry, only this packaging is currently provided. Contribution welcome), on the [release pages](https://github.com/BROADSoftware/logtrawler/releases).

There are several package, which differ by HBase and Hadoop library version. Pick up the one appropriate for your context.

Once installed, usage is the following:

    $ jdchtable --inputFile yourDescription.yml
    
Where yourDescription.yml is a file containing your target HBase namespace and table description. jdchtable will then perform all required operation to reach this target state.

Note than if yourDescription.yml match the current configuration, no operation will be performed.

Here is a sample of such description.yml file:

    zookeeper: "zknode1.yourdomain.com,zknode2.yourdomain.com,zknode3.yourdomain.com"
    znodeParent: /hbase-unsecure
    namespaces: 
      - name: testapp1
        tables:
        - name: testtable1
          properties: 
            regionReplication: 1
            durability: ASYNC_WAL
          columnFamilies:
          - name: cf1
            properties: 
              cacheBloomsOnWrite: true
              compressionType: NONE
          - name: cf2
            properties:
              maxVersions: 12
              minVersions: 2
              timeToLive: 200000
          presplit:
            keysAsString:
            - BENJAMIN
            - JULIA
            - MARTIN
            - PAUL
            - VALENTIN       

* zookeeper: Must be filled with the zookeeper's quorum of your target cluster. 

> Note this parameter may be missing. In such case, you will need to provide the --zookeeper parameter on the command line.

* znodeParent: Must match the value of the property `zookeeper.znode.parent` in `hbase-site.xml`. Should be `/hbase` in most case, `/hbase-unsecure` on some HDP cluster.

> This parameter may also be missing. In such case, it will default to '/hbase'. You may also provide this value as a command line option.

* namespaces: This tag introduce a list of namespaces, each one with a `name:` attribute and hosting one or several tables, under the `tables: attribute
 
> If you don't want to use namespaces, simply define one with `name: default`

* Then, each table is described with:

>* A name: attribute, providing the table name.

>* A list of properties, allowing definition of table properties.

>* A list of columnFamilies, with, for each a name and a set of properties.

>* Optionally, a presplit block, allowing an initial region split schema to be defined.

### Properties reference

You will find table properties definition is HBase documentation. For a complete list, please refer to the Javadoc of the class `org.apache.hadoop.hbase.HTableDescriptor` of your HBase version.

For the columnFamily properties. Refer to the javadoc of the class `org.apache.hadoop.hbase.HColumnDescriptor`.

### presplit attribute

Please, note than unlike all other definition, presplitting is only effective at the initial table creation. If the table already exists, no modification is performed and the presplit: attribute is ignored.

Presplitting can be expressed with one othe the following 3 methods:

	  presplit:
	    keysAsString:
	    - BENJAMIN
	    - JULIA
	    - MARTIN
	    - PAUL
	    - VALENTIN       

or:

	  presplit:
	    keysAsNumber:
	    - 20000
	    - 40000
	    - 60000
	    - 80000

or:
            
	  presplit:
	    startKey: 10000
	    endKey: 90000
	    numRegion: 10
            

### Other launch option

When launching the jdchtable command you may provide some optional parameters:

* --zookeeper parameter will allow to override the value in the description.yml file.

* --znodeParent parameter will allow to override the value in the description.yml file.

### ColumnFamily, Table and namespace deletion

All namespace, table or columnFamiliy not described in the description.yml file will be left untouched.

To allow deletion to be performed, All theses object got a `state:` attribute. When not defined, it default to 'present'. But it could be set to 'absent' to trigger the deletion of the corresponding entity.

For example: 

     namespaces: 
      - name: testapp1
        tables:
        - name: testtable1
          properties: 
            regionReplication: 1
            durability: ASYNC_WAL
          columnFamilies:
          - name: cf1
            properties: 
              cacheBloomsOnWrite: true
              compressionType: NONE
          - name: cf2
            state: absent

Will delete columnFamily `cf2` (if existing) from previous configuration. 

And:

     namespaces: 
      - name: testapp1
        state: absent
        tables:
        - name: testtable1
          state: absent

Will remove all object created by our previous example.

> Note, as a security, no cascading deletion from namespace to table will be performed. Deletion of a namespace can only be effective if all hosted table are explicitly deleted. 

***
## Ansible integration

With its idempotence property, jdchtable is very easy to be orchestrated by usual DevOps tools like Chef, Puppet or Ansible.

You will find an Ansible role [at this location](http://github.com/BROADSoftware/bsx-roles/tree/master/hadoop/jdchtable).

This role can be used as following;

	
	- hosts: zookeepers
	
	- hosts: sr1
	  vars:
	    jdchtable_rpm_url: https://github.com/BROADSoftware/jdchtable/releases/download/v0.1.0/jdchtable_cdh552-0.1.0-1.noarch.rpm
	
	    myDescription:
	      namespaces: 
	      - name: testapp1
	        tables:
	        - name: testtable1
	          properties: 
	            regionReplication: 1
	            durability: ASYNC_WAL
	          columnFamilies:
	          - name: cf1
	            properties: 
	              cacheBloomsOnWrite: true
	              compressionType: NONE
    
	  roles:
	  - { role: hadoop/jdchtable, jdchtable_description: "{{myDescription}}" }
  
> Note `- hosts: zookeepers` at the beginning, which force ansible to grab info about the hosts in the [zookeepers] group, to be able to fulfill this info into jdchtable configuration. Of course, such a group must be defined in the inventory. 

***
## Build

Just clone this repository and then:

    $ gradlew all

This should build everything. You should be able to find generated packages in build/distribution folder.

***
## License

    Copyright (C) 2016 BROADSoftware

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at
	
	    http://www.apache.org/licenses/LICENSE-2.0
	
	Unless required by applicable law or agreed to in writing, software
	distributed under the License is distributed on an "AS IS" BASIS,
	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
	See the License for the specific language governing permissions and
	limitations under the License.

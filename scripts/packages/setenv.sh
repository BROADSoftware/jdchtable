
# Set JAVA_HOME. Must be at least 1.7.
# If not set, will try to lookup a correct version.
# JAVA_HOME=/some/place/where/to/find/java

# Set the log configuration file
JOPTS="$JOPTS -Dlog4j.configuration=file:/etc/jdchtable/log4j.xml"

# Dump configuration, for debugging
OPTS="$OPTS --dumpConfigFile /tmp/jdchtable-conf.txt"

# Set kerberos principal and keytab (If you don't like 'kinit')
# OPTS="$OPTS --principal hbase-mycluster --keytab /etc/security/keytabs/hbase.headless.keytab

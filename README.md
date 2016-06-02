basic framework for reading and writing from kafka and hbase

    To run:

    mvn clean compile exec:java -Dexec.mainClass="com.frameworks.storm.topology.StsAgrTopology" -Dexec.args="storm-one-framework-1.0.0-SNAPSHOT-storm.jar"
storm jar target/storm-one-framework-1.0.0-SNAPSHOT-storm.jar com.frameworks.storm.topology.StsAgrTopology

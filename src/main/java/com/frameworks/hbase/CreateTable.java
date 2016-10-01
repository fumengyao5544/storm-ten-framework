package com.frameworks.hbase;

import java.io.IOException;
import java.io.InputStream;

import lombok.Setter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.conf.Configuration;
import org.yaml.snakeyaml.Yaml;

@Slf4j
@Setter
public class CreateTable {

    String zkQuorum;
    String znodeParent;
    String clientPort;
    String tableName;
    String family;

    @SneakyThrows
    public void createTable(){

        // Instantiating configuration class
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", zkQuorum);
        config.set("zookeeper.znode.parent",znodeParent);
        //config.set("hbase.zookeeper.property.clientPort", clientPort);

        // Instantiating HbaseAdmin class
        HBaseAdmin admin = new HBaseAdmin(config);
        // Instantiating table descriptor class
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        if(admin.tableExists(tableName)){
            log.error("table exists!");
            return;
        }


        // Adding column families to table descriptor
        tableDescriptor.addFamily(new HColumnDescriptor(family));
        // Execute the table through admin
        admin.createTable(tableDescriptor);
        System.out.println(" Table created ");
    }

    @SneakyThrows
    public static void main(String[] args) throws IOException {

        //look in yml file for credentials
        Yaml yaml = new Yaml();
        InputStream in = ClassLoader.getSystemResourceAsStream("hbase-credentials.yml");
        CreateTable tableCreator= yaml.loadAs(in, CreateTable.class);
        tableCreator.createTable();
    }
}
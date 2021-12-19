import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.File;
import java.io.IOException;

public class HBaseService {
    private static HBaseService instance = null;
    private Configuration conf;
    private Connection conn;
    private Admin admin;

    public HBaseService() throws IOException {
        this.conf  = HBaseConfiguration.create();
        // Must add two lines below to run on AWS EMR, comment out if run locally
        String hBaseSite = "/etc/hbase/conf/hbase-site.xml";
        conf.addResource(new File(hBaseSite).toURI().toURL());
        this.conn = ConnectionFactory.createConnection(conf);
        this.admin = this.conn.getAdmin();
    }

    public static HBaseService createHBaseService() throws IOException {
        if(instance == null){
            instance = new HBaseService();
        }
        return instance;
    }

    public Table createTable(String tableName, String colFamily) throws IOException {
        if(admin.tableExists(TableName.valueOf(tableName))){
            System.out.println("Table exists: " + tableName);
        } else{
            ColumnFamilyDescriptor columnFamily = ColumnFamilyDescriptorBuilder.of(colFamily);
            TableDescriptor descriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
                    .setColumnFamily(columnFamily).build();
            this.admin.createTable(descriptor);
            System.out.println("Created new table: " + tableName);
        }
        return this.conn.getTable(TableName.valueOf(tableName));
    }

    public Table getTable(String tableName) throws IOException {
        if(admin.tableExists(TableName.valueOf(tableName))){
            System.out.println("Table exists, fetching: " + tableName);
            return this.conn.getTable(TableName.valueOf(tableName));
        } else{
            System.out.println("Table doesn't exist: " + tableName);
            return null;
        }
    }

    public void close() throws IOException {
        this.conn.close();
        this.admin.close();
    }
    public Configuration getConf() {
        return conf;
    }
}

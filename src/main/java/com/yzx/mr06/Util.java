package com.yzx.mr06;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

/**
 * 获取连接
 *
 * @author Administrator
 */
public class Util {

    static Connection conn = null;


    //获取连接

    public static Connection getConn() {
        Configuration conf = new Configuration();
        conf.addResource("hbase-site.xml");
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static void ConnClose() {

        try {
            if (conn != null) {
                conn.close();
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public static Admin getAdmin() {

        Admin admin = null;
        Connection conn = getConn();
        try {
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return admin;
    }






    public static void createTable() {
        //如果表不存在就创建表  hospitalid  diseaseid  hospitalid+diseaseid
        Logger logger = Logger.getLogger("hadoop.log");
        Admin admin = null;
        try {
            admin = Util.getConn().getAdmin();
            boolean table_exist = admin.tableExists(TableName.valueOf("mr_table"));
            if (table_exist != true) {
                logger.warning(table_exist + "is not exist,create");
                HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("mr_table"));
                HColumnDescriptor hospitalid = new HColumnDescriptor("hospitalid");
                HColumnDescriptor diseaseid = new HColumnDescriptor("diseaseid");
                HColumnDescriptor bothid = new HColumnDescriptor("hospitalid+diseaseid");
                hTableDescriptor.addFamily(hospitalid);
                hTableDescriptor.addFamily(diseaseid);
                hTableDescriptor.addFamily(bothid);
                admin.createTable(hTableDescriptor);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static HashMap<String,Double> getDatas(Table table,String rowKeyName, String familyName) {

        HashMap<String,Double> kvData = new HashMap<>();
        double num = 0;
        Get get = new Get(Bytes.toBytes(rowKeyName));
        get.addFamily(familyName.getBytes());
        try {
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                String k = new String(CellUtil.cloneQualifier(cell));
                double v = Double.parseDouble(new String(CellUtil.cloneValue(cell)));
                kvData.put(k,v);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return kvData;
    }




}

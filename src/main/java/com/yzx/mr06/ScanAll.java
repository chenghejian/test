package com.yzx.mr06;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import sun.applet.Main;

import java.io.IOException;

public class ScanAll {

    public static void scanAll(String tableName) {
        try {
            Table table = Util.getConn().getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                for (Cell cell : result.rawCells()) {
                    System.out.println(new String(CellUtil.cloneRow(cell)) + "\t"
                            + new String(CellUtil.cloneFamily(cell)) + "\t" + new String(CellUtil.cloneQualifier(cell))
                            + "\t" + new String(CellUtil.cloneValue(cell), "UTF-8"));
                }

            }
            table.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }


    public static void main(String[] args) {
        ScanAll.scanAll("mr_table");


    }

}

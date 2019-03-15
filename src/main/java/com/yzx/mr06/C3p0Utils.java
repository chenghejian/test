package com.yzx.mr06;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class C3p0Utils {
    static org.apache.log4j.Logger logger=org.apache.log4j.Logger.getLogger(C3p0Utils.class.getName());

    //通过标识名来创建相应连接池
    static ComboPooledDataSource pool=new ComboPooledDataSource("mysql");
    //从连接池中取用一个连接
    public static Connection getConnection(){
        try {
            return pool.getConnection();
        } catch (SQLException e) {
            logger.error("Exception in C3p0Utils!", e);
            e.printStackTrace();
            return null;
        }
    }
    //释放连接回连接池
    public static void close(Connection conn, PreparedStatement pst, ResultSet rs){
        if(rs!=null){
            try {
                rs.close();
            } catch (SQLException e) {
                logger.error("Exception in C3p0Utils!", e);
            }
        }
        if(pst!=null){
            try {
                pst.close();
            } catch (SQLException e) {
                logger.error("Exception in C3p0Utils!", e);
            }
        }

        if(conn!=null){
            try {
                conn.close();
            } catch (SQLException e) {
                logger.error("Exception in C3p0Utils!", e);
            }
        }
    }
}
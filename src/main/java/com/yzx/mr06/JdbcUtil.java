package com.yzx.mr06;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

public class JdbcUtil {

    private static String user;
    private static String password;
    private static String url;

    static{

        try {
            Class.forName("com.mysql.jdbc.Driver");
            JdbcUtil u = new JdbcUtil();
            InputStream in = u.getClass().getClassLoader().getResourceAsStream("jdbc.properties");
            Properties p = new Properties();
            try {
                p.load(in);
                user = p.getProperty("user");
                password = p.getProperty("password");
                url = p.getProperty("url");
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public static Connection getJdbcConn(){
        try {
            Connection conn = DriverManager.getConnection(url, user, password);
            return conn;
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

    public static void JdbcClose(ResultSet res, Statement sta, Connection conn){
        if(null!=res){
            try {
                res.close();
                if(null!=sta){
                    sta.close();
                    if(null!=conn){
                        conn.close();
                    }
                }
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public static void insert(String id,double hcount,double havgcost,double havgreimburse,double havgreproportion,double havgday,double havgfinproportion,double ocount,double oavgcost,double oavgreimburse,double oavgreproportion,double oavgfinproportion){
        Connection jdbcConn = getJdbcConn();
        String sql = "insert into test values(?,?,?,?,?,?,?,?,?,?,?,?)";
        try {
            PreparedStatement pr = jdbcConn.prepareStatement(sql);
            pr.setString( 1, id);
            pr.setInt( 2, (int)hcount);
            pr.setDouble( 3, havgcost);
            pr.setDouble( 4, havgreimburse);
            pr.setString( 5, havgreproportion+"%");
            pr.setInt( 6, (int)havgday);
            pr.setString( 7, havgfinproportion+"%");
            pr.setInt( 8, (int)ocount);
            pr.setDouble( 9, oavgcost);
            pr.setDouble( 10, oavgreimburse);
            pr.setString( 11, oavgreproportion+"%");
            pr.setString( 12, oavgfinproportion+"%");
            pr.executeUpdate();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}

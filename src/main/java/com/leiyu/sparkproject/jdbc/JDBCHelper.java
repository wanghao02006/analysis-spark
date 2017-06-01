package com.leiyu.sparkproject.jdbc;

import com.leiyu.sparkproject.conf.ConfigurationManager;
import com.leiyu.sparkproject.constants.Constants;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by wh on 2017/5/26.
 */
public class JDBCHelper {

    private static volatile JDBCHelper instance = null;

    private LinkedList<Connection> datasource = new LinkedList<>();

    static {
        try {
            String driver = ConfigurationManager.getProperty(Constants.JDBC_DRIVER);
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    private JDBCHelper(){
        int datasourceSize = ConfigurationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE);
        String url = ConfigurationManager.getProperty(Constants.JDBC_URL);
        String user = ConfigurationManager.getProperty(Constants.JDBC_USER);
        String password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);

        for(int i = 0 ; i < datasourceSize ; i++){
            try {
                Connection connection = DriverManager.getConnection(url,user,password);
                this.datasource.push(connection);
            } catch (SQLException e) {
                throw new RuntimeException("init Connection Pool failed!");
            }
        }

    }

    public static JDBCHelper getInstance(){
        if(null == instance){
            synchronized (JDBCHelper.class){
                if(null == instance){
                    instance = new JDBCHelper();
                }
            }
        }

        return instance;
    }

    public synchronized Connection getConnection(){
        while (datasource.size() == 0){
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return this.datasource.poll();
    }

    public int executeUpdate(String sql,Object[] params){
        int rtn = 0 ;
        Connection conn = null ;
        PreparedStatement statement = null ;
        try{
            conn = getConnection();
            statement = conn.prepareStatement(sql);
            for(int i = 0 ; i < params.length ; i++){
                statement.setObject(i + 1,params[i]);
            }
            rtn = statement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if (null != conn){
                datasource.push(conn);
            }
        }
        return rtn;
    }

    public void executeQuery(String sql , Object[] params, QueryCallBack callback){
        Connection conn = null;
        PreparedStatement statement = null;
        ResultSet rs = null;
        try{
            conn = getConnection();
            statement = conn.prepareStatement(sql);

            for (int i = 0 ; i < params.length ; i++){
                statement.setObject(i + 1,params[i]);
            }

            rs = statement.executeQuery();
            callback.process(rs);
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if (null != conn){
                datasource.push(conn);
            }
        }
    }

    public int[] executeBatch(String sql, List<Object[]> paramList){
        int[] rtn = null;
        Connection conn = null;
        PreparedStatement statement = null;
        try {
            conn = getConnection();

            conn.setAutoCommit(false);
            statement = conn.prepareStatement(sql);

            for (Object[] params : paramList){
                for(int i = 0 ; i < params.length ; i++){
                    statement.setObject(i + 1,params[i]);
                }
                statement.addBatch();
            }
            rtn = statement.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rtn;
    }

    public static interface QueryCallBack {
        public void process(ResultSet rs) throws SQLException;
    }
}

package com.leiyu.sparkproject.jdbc;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by wh on 2017/6/1.
 */

public class TestJDBCHelper {

    @Test
    public void testexecuteUpdate(){
        JDBCHelper helper = JDBCHelper.getInstance();
        int result = helper.executeUpdate("insert into test_user (name,age) values (?,?)",new Object[]{"王浩",29});
        assertEquals(1,result);
    }

    @Test
    public void testexecuteQuery(){
        JDBCHelper helper = JDBCHelper.getInstance();
        final Map<String,Object> testUser = new HashMap<>();

        helper.executeQuery("select name,age from test_user where name = ?", new Object[]{"王浩"}, new JDBCHelper.QueryCallBack() {
            @Override
            public void process(ResultSet rs) throws SQLException {
                if(null != rs && rs.next()){
                    String name = rs.getString(1);
                    int age = rs.getInt(2);
                    testUser.put("name",name);
                    testUser.put("age",age);
                }

            }
        });

        assertEquals(29,testUser.get("age"));
    }
    @Test
    public void executeBatch(){
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        String sql = "insert into test_user(name,age) values(?,?)";

        List<Object[]> paramsList = new ArrayList<Object[]>();
        paramsList.add(new Object[]{"麻子", 30});
        paramsList.add(new Object[]{"王五", 35});

        jdbcHelper.executeBatch(sql, paramsList);
    }

}

package com.mysql2mysql;

/**
 * Created by admin on 2019/6/8.
 */

import com.flink.utils.JdbcUtil;
import entity.UserInfo;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

public class JdbcReader extends RichSourceFunction<Tuple3<String,String,String>> {
    private static final Logger logger = LoggerFactory.getLogger(JdbcReader.class);

    private Connection connection = null;
    private PreparedStatement ps = null;


    String sql = "select *  from user_info";


    @Override
    public void open(Configuration parameters) throws Exception {
        connection = JdbcUtil.getConnection();
        ps = connection.prepareStatement(sql);
    }

    //执行查询并获取结果
    @Override
    public void run(SourceContext<Tuple3<String, String, String>> ctx) throws Exception {
        try
        {
            List<UserInfo> resultSet = JdbcUtil.query(sql, new String[]{}, UserInfo.class);
            for (UserInfo userInfo : resultSet)
            {
                String username = userInfo.getUsername();
                String password = userInfo.getPassword();
                String id = userInfo.getId();
                logger.error("readJDBC name:{}", username + "==" + id + "==" + resultSet);
                Tuple3<String, String, String> tuple3 = new Tuple3<String, String, String>();
                tuple3.setFields(id, username, password);
                ctx.collect(tuple3);
            }
        } catch (Exception e) {
            logger.error("runException:{}", e);
        }

    }

    //关闭数据库连接
    @Override
    public void cancel() {
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            logger.error("runException:{}", e);
        }
    }
}

package com.mysql2mysql;

/**
 * Created by admin on 2019/6/8.
 */
import com.flink.utils.JdbcUtil;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;


public class JdbcWriter extends RichSinkFunction<Tuple3<String,String,String>> {
    private Connection connection;
    private PreparedStatement preparedStatement;

    String sql = "insert into user_info1(id,username, password) VALUES(?,?,?)";

    @Override
    public void open(Configuration parameters) throws Exception {

        // 获取数据库连接
        connection = JdbcUtil.getConnection();
        preparedStatement = connection.prepareStatement(sql);//insert sql在配置文件中
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(preparedStatement != null){
            preparedStatement.close();
        }
        if(connection != null){
            connection.close();
        }
        super.close();
    }

    @Override
    public void invoke(Tuple3<String, String,String> stringStringTuple3) throws Exception {
        try {
            String id = stringStringTuple3.getField(0);
            String username = stringStringTuple3.getField(1);
            String password = stringStringTuple3.getField(2);
            preparedStatement.setString(1,id);
            preparedStatement.setString(2,username);
            preparedStatement.setString(3,password);
            preparedStatement.executeUpdate();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}


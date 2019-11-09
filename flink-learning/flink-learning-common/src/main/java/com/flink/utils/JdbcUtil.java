package com.flink.utils;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.beanutils.BeanUtils;


/**
 * 通用的jdbc工具类
 *\获取连接\关闭连接\查询\更新
 * @author wangguoqing
 *
 */
public class JdbcUtil {

    private static String url = null ;

    private static String user = null ;

    private static String password = null ;

    private static String driverClass = null ;

    static{
        try{
            Properties prop = new Properties() ;
            InputStream in = JdbcUtil.class.getResourceAsStream("/jdbc.properties") ;
            prop.load(in);
            url = prop.getProperty("jdbc.url") ;
            user = prop.getProperty("jdbc.user") ;
            password = prop.getProperty("jdbc.password") ;
            driverClass = prop.getProperty("jdbc.driver") ;
            Class.forName(driverClass) ;
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    /**
     * 建立连接
     * @return
     */
    public static Connection getConnection(){
        try {
            Connection conn = DriverManager.getConnection(url,user,password) ;
            return conn ;
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e) ;
        }
    }
    /**
     * 判断是否有查询的数据
     * @param sql
     * @param values sql中对应的参数值
     * @param values Bean对象的Class
     * @return
     */
    public static boolean query(String sql,Object[] values) {
        Connection con=null;
        PreparedStatement pstmt=null;
        ResultSet rs=null;
        try {
            con=DriverManager.getConnection(url,user,password);
            pstmt=con.prepareStatement(sql);
            ParameterMetaData pmd = pstmt.getParameterMetaData();
            int parameterCount=pmd.getParameterCount();
            for(int i=1;i<=parameterCount;i++) {
                pstmt.setObject(i,values[i-1]);
            }
            rs=pstmt.executeQuery();
            if(rs.next()) {
                return true;
            }
            else {
                return false;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
        finally {
            JdbcUtil.close(pstmt, con, rs);;
        }
    }
    /**
     * 返回查询的结果
     * @param sql
     * @param values
     * @param clazz
     * @return
     */
    public static<T> List<T> query(String sql,Object[] values,Class<T> clazz) {
        Connection con=null;
        PreparedStatement pstmt=null;
        ResultSet rs=null;
        try {
            con=DriverManager.getConnection(url,user,password);
            pstmt=con.prepareStatement(sql);
            ParameterMetaData pmd = pstmt.getParameterMetaData();
            int parameterCount=pmd.getParameterCount();

            if(values!=null) {
                for(int i=1;i<=parameterCount;i++) {
                    pstmt.setObject(i,values[i-1]);
                }
            }

            rs=pstmt.executeQuery();
            ResultSetMetaData rsmd = rs.getMetaData();
            int count=rsmd.getColumnCount();
            List li=new ArrayList();
            while(rs.next()) {
                Object obj=clazz.newInstance();
                for(int i=1;i<=count;i++) {
                    Object value=rs.getObject(i);
                    BeanUtils.copyProperty(obj,rsmd.getColumnName(i),value);

                }
                li.add(obj);
            }
            return li;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
        finally {
            JdbcUtil.close(pstmt, con, rs);;
        }

    }
    /**
     * 通用的Jdbc更新操作：插入\删除\修改
     * @param sql
     * @param values
     */
    public static void update(String sql,Object[] values) {
        Connection con=null;
        PreparedStatement pStmt=null;
        try {
            con=DriverManager.getConnection(url,user,password);
            pStmt = con.prepareStatement(sql);
            ParameterMetaData pmd=pStmt.getParameterMetaData();
            int count=pmd.getParameterCount();

            for(int i=1;i<=count;i++) {
                pStmt.setObject(i,values[i-1]);
            }
            pStmt.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException();
        }
        finally {
            JdbcUtil.close(pStmt, con);
        }
    }
    public static void close(Statement stmt,Connection con) {
        if(stmt!=null) {
            try {
                stmt.close();
            }
            catch(SQLException e) {
                e.printStackTrace();
            }
            finally {
                if(con!=null) {
                    try {
                        con.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public static void close(PreparedStatement pstmt,Connection con,ResultSet rs){
        if(pstmt!=null){
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e) ;
            }
            finally{
                if(con!=null){
                    try {
                        con.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e) ;
                    }
                    finally {
                        if(rs!=null){
                            try {
                                rs.close();
                            } catch (SQLException e) {
                                e.printStackTrace();
                                throw new RuntimeException(e) ;
                            }
                        }

                    }
                }
            }
        }

    }
}

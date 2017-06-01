package com.leiyu.sparkproject.conf;



import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by wh on 2017/5/26.
 */
public class ConfigurationManager {


    private static Properties prop = new Properties();


    static {
        InputStream in = null;
        try {
            in = ConfigurationManager.class.getClassLoader().getResourceAsStream("my.properties");
            prop.load(in);
        } catch (IOException e) {
            System.out.println("load properties faild!");
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public static String getProperty(String key){
        return prop.getProperty(key);
    }

    public static Integer getInteger(String key){
        try {
            String strResult = getProperty(key);
            return Integer.parseInt(strResult);
        }catch (Exception e){
            throw new RuntimeException("can't parse to Integer!");
        }
    }


}

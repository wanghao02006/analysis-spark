package com.leiyu.sparkproject.conf;

import org.junit.Test;

import static org.junit.Assert.*;
/**
 * Created by wh on 2017/5/26.
 */
public class ConfigurationManagerTest {

    @Test
    public void testGetProperty(){
        assertEquals("abc",ConfigurationManager.getProperty("testkey1"));
        assertEquals("def",ConfigurationManager.getProperty("testkey2"));
    }
}

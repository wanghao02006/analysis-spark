package com.leiyu.sparkproject.dao.impl;

import com.leiyu.sparkproject.dao.ISessionAggrStatDAO;
import com.leiyu.sparkproject.dao.ITaskDAO;

/**
 * Created by wh on 2017/6/8.
 */
public class DAOFactory {

    public static ITaskDAO getTaskDAO(){
        return new TaskDAOImpl();
    }


    public static ISessionAggrStatDAO getSessionAggrStatDAO(){
        return new SessionAggrStatDAOImpl();
    }

}

package com.leiyu.sparkproject.dao;
import static org.junit.Assert.*;

import com.leiyu.sparkproject.dao.impl.DAOFactory;
import com.leiyu.sparkproject.dao.impl.TaskDAOImpl;
import com.leiyu.sparkproject.domain.Task;
import org.junit.Test;

/**
 * Created by wh on 2017/6/8.
 */
public class TestTaskDao {

    @Test
    public void testfindById(){
        ITaskDAO dao = new TaskDAOImpl();
        Task task = dao.findById(1);
        assertEquals("Task1",task.getTaskName());
    }

    @Test
    public void testFactory(){
        ITaskDAO dao = DAOFactory.getTaskDAO();
        Task task = dao.findById(1);
        assertEquals("Task1",task.getTaskName());
    }

}

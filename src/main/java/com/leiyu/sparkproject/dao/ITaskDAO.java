package com.leiyu.sparkproject.dao;

import com.leiyu.sparkproject.domain.Task;

/**
 * Created by wh on 2017/6/1.
 */
public interface ITaskDAO {

    Task findById(long taskid);
}

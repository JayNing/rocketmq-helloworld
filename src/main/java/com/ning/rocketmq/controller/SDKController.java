package com.ning.rocketmq.controller;

import com.dynamic.code.bean.Person;
import com.dynamic.code.bean.User;
import com.dynamic.code.bean.table.TableA;
import com.dynamic.code.service.PersonService;
import com.util.GsonUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * author JayNing
 * created by 2020/5/15 10:02
 **/
@RestController
@RequestMapping("sdk")
public class SDKController {

    @Autowired
    private PersonService personService;

    @RequestMapping("request")
    public String request(){
        Person person = new Person();
        person.setAge(18L);
        person.setUsername("朱元璋");
        person.setBirthday(new Date());
        List<TableA> aList = new ArrayList<>();
        TableA tableA = new TableA();
        tableA.setAaa("1111");
        tableA.setBbb(2222L);
        aList.add(tableA);
        person.setTableA(aList);

        return GsonUtils.toJsonString(personService.requestPerson(person));
    }

    @RequestMapping("request2")
    public String request2(){
        Person person = new Person();
        person.setAge(18L);
        person.setUsername("朱元璋2");
        person.setBirthday(new Date());
        List<TableA> aList = new ArrayList<>();
        TableA tableA = new TableA();
        tableA.setAaa("333");
        tableA.setBbb(3333333333L);
        aList.add(tableA);
        person.setTableA(aList);

        return GsonUtils.toJsonString(personService.requestUser(person));
    }

}

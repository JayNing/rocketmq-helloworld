package com.ning.rocketmq.service.impl;

import com.ning.rocketmq.service.UserService;
import org.springframework.stereotype.Service;

/**
 * author JayNing
 * created by 2020/2/20 19:41
 **/
@Service("userService")
public class UserServiceImpl implements UserService {
    @Override
    public String getUser(String username) {
        return "{范闲}";
    }
}

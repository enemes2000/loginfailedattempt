package com.github.enemes2000.controller;



import com.github.enemes2000.model.LoginFailCount;
import com.github.enemes2000.service.LoginAttemptService;
import com.github.enemes2000.topology.LoginTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


import java.util.List;


@RestController
@RequestMapping("/login")
public class LoginAttemptFailController {
    Logger LOGGER = LoggerFactory.getLogger(LoginAttemptFailController.class);

    @Autowired
    private LoginAttemptService service;

    @RequestMapping("/failattempt")
    public List<LoginFailCount> failCount(@RequestParam("username") String username){
        LOGGER.info("Get the number of fails login for username: %s", username);
        return service.getLoginFailCount(username);
    }


}

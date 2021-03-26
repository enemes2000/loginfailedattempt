package com.github.enemes2000.controller;



import com.github.enemes2000.model.LoginFailCount;
import com.github.enemes2000.service.LoginAttemptService;
import com.github.enemes2000.topology.LoginTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


import java.time.Instant;
import java.util.List;


@RestController
@RequestMapping("/login")
@Profile("dev")
public class LoginAttemptFailController {
    Logger LOGGER = LoggerFactory.getLogger(LoginAttemptFailController.class);

    @Autowired
    private LoginAttemptService service;

    @RequestMapping("/failattempt")
    public List<LoginFailCount> failCount(@RequestParam("username") String username){
        LOGGER.info("Get the number of fails login for username: %s", username);
        String from = "2021-03-17T09:02:00.000Z";
        String to = "2021-03-17T09:02:02.500Z";
        return service.getLoginFailCount(username, from, to);
    }

    @RequestMapping("/failattempt/all")
    public List<LoginFailCount> allFailCount(){
        LOGGER.info("Get all of the fails login for username");
        String from = "2021-03-17T09:02:00.000Z";
        String to = "2021-03-17T09:02:02.500Z";
        return service.getAllFromRange(from, to);
    }


}

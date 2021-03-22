package com.github.enemes2000.model;



public class LoginFailCount implements Comparable<LoginFailCount>{

    private String username;

    private long failattemptCount;

    private String ipAddress;

    public String getUsername() {
        return username;
    }

    public long getFailattemptCount() {
        return failattemptCount;
    }

    public String getIpAddress() {
        return ipAddress;
    }


    public LoginFailCount(String username, long failattemptCount, String ipAddress){
        this.username = username;
        this.failattemptCount = failattemptCount;
        this.ipAddress = ipAddress;
    }

    @Override
    public String toString() {
        return String.format("LoginFailCount(%s,%d,%s)",username,failattemptCount,ipAddress);
    }

    @Override
    public int compareTo(LoginFailCount o) {
        if (this.username.equals(o.username) || this.ipAddress.equals(o.ipAddress)) return 0;
        return -1;
    }
}

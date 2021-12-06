package com.chriszt.flink.sql.streamintegration;

public class User {

    private int id;

    private String clazz;

    private String name;

    private int age;

    public User() {
    }

    public User(int id, String clazz, String name, int age) {
        this.id = id;
        this.clazz = clazz;
        this.name = name;
        this.age = age;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getClazz() {
        return clazz;
    }

    public void setClazz(String clazz) {
        this.clazz = clazz;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "User{id:" + id +
                ",clazz=" + clazz +
                ",name=" + name +
                ",age=" + age +
                "}";
    }
}

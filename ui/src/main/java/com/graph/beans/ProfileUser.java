package com.graph.beans;

/**
 * Created by saurav on 06/03/17.
 */
public class ProfileUser {
    Long Id;
    String name;

    public Long getId() {
        return Id;
    }

    public void setId(Long id) {
        Id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "ProfileUser{" +
                "Id=" + Id +
                ", name='" + name + '\'' +
                '}';
    }
}

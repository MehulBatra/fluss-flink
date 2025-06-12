package com.common;

import java.io.Serializable;
import java.util.Objects;

public class Person implements Serializable {
    private static final long serialVersionUID = 1L;

    public Long id;
    public String name;
    public Integer age;
    public Double score;
    public Long processedTime;

    public Person() {}

    public Person(Long id, String name, Integer age, Double score) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.score = score;
    }

    public Person(Long id, String name, Integer age, Double score, Long processedTime) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.score = score;
        this.processedTime = processedTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Person person = (Person) o;
        return Objects.equals(id, person.id) &&
                Objects.equals(name, person.name) &&
                Objects.equals(age, person.age) &&
                Objects.equals(score, person.score) &&
                Objects.equals(processedTime, person.processedTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, age, score, processedTime);
    }

    @Override
    public String toString() {
        return "Person{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", age=" + age +
                ", score=" + score +
                ", processedTime=" + processedTime +
                '}';
    }
}
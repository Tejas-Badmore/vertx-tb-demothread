package com.example.vertx_tb_demothread;

import io.vertx.core.json.JsonObject;

import java.util.concurrent.atomic.AtomicInteger;

public class Employee {
  private static final AtomicInteger COUNTER = new AtomicInteger();


  private String _id;
  private String name;
  private String age;
  private String team;
  private String doj;

  public Employee(String name, String age, String team, String doj) {
    this.name = name;
    this.age = age;
    this.team = team;
    this.doj = doj;
  }
  public Employee(JsonObject json) {
    this.name = json.getString("name");
    this.age = json.getString("age");
    this._id = json.getString("_id");
    this.team = json.getString("team");
    this.doj = json.getString("doj");
  }

  public String getName() {
    return name;
  }

  public String getAge() {
    return age;
  }


  public void setName(String name) {
    this.name = name;
  }

  public void setAge(String age) {
    this.age = age;
  }

  public static int getMaxId(){
    return COUNTER.get();
  }
  public JsonObject toJson() {
    JsonObject json = new JsonObject()
      .put("name", name)
      .put("age", age)
      .put("team",team)
      .put("doj",doj);
    return json;
  }
}

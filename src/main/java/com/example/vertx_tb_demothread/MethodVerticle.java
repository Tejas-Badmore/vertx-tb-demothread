package com.example.vertx_tb_demothread;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;

public class MethodVerticle extends AbstractVerticle {
  public static final String COLLECTION_NAME = "Employees";
  @Override
  public void start(){
    MongoClient mongo= MongoClient.createShared(vertx, config());
//    vertx.eventBus().consumer("post-address",msg->{
//      JsonObject jsonObject = JsonObject.mapFrom(msg.body());
//      String name = jsonObject.getString("name");
//      Employee emp = new Employee(jsonObject.getString("name"),jsonObject.getString("age"));
//      EmpObj.products.put(emp.getId(), emp);
//      System.out.println("Post mai ayya");
//      msg.reply(Json.encodePrettily(emp));
//    });
    //post one
    vertx.eventBus().consumer("post-address",msg->{
      JsonObject jsonObject = JsonObject.mapFrom(msg.body());
      Employee emp = new Employee(jsonObject.getString("name"),jsonObject.getString("age")
        ,jsonObject.getString("team"),jsonObject.getString("doj"));
      System.out.println("Post mai ayya: "+Thread.currentThread().getName());
      mongo.insert(COLLECTION_NAME, emp.toJson(), result ->{
        if (result.succeeded())
          msg.reply(Json.encodePrettily(emp));
        else
          msg.reply("Failed to Insert");
      });

    });

    //get one
    vertx.eventBus().consumer("get-one-address",msg->{
      System.out.println("Get one mai ayya: "+Thread.currentThread().getName());
      String id = msg.body().toString();
      if (id == null)
        msg.reply("Dirty Input");
      else {                                          //.put("id", Integer.parseInt(id))
        mongo.findOne(COLLECTION_NAME, new JsonObject().put("_id",id),new JsonObject(), results -> {
          if(results.result()!=null){
            Employee employee = new Employee(results.result());
            msg.reply(Json.encodePrettily(results.result()));
          }
          else
            msg.reply("Document not present");
        });
      }
    });
  }
}

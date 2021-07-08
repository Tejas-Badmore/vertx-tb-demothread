package com.example.vertx_tb_demothread;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.FindOptions;
import io.vertx.ext.mongo.MongoClient;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class WorkerVerticle extends AbstractVerticle {
//  private static Map<Integer, Employee> products = new HashMap<>();

  public static final String COLLECTION_NAME = "Employees";
  MongoClient mongo;
  @Override
  public void start(Promise<Void> promise){
     mongo= MongoClient.createShared(vertx, config());
//    createSomeData();
    //post
//    vertx.eventBus().consumer("post-address",msg->{
//      JsonObject jsonObject = JsonObject.mapFrom(msg.body());
//      String name = jsonObject.getString("name");
//      Employee emp = new Employee(jsonObject.getString("name"),jsonObject.getString("age"));
//      EmpObj.products.put(emp.getId(), emp);
//      System.out.println("Post mai ayya");
//      msg.reply(Json.encodePrettily(emp));
//    });

    //get all
    vertx.eventBus().consumer("get-all-address",msg->{
      System.out.println("Get all mai ayya: "+Thread.currentThread().getName());
      mongo.find(COLLECTION_NAME, new JsonObject(), results -> {
        List<JsonObject> objects = results.result();
        List<Employee> employees = objects.stream().map(Employee::new).collect(Collectors.toList());
        msg.reply(Json.encodePrettily(objects));
      });
    });

//    //get one
//    vertx.eventBus().consumer("get-one-address",msg->{
//      System.out.println("Get one mai ayya: "+Thread.currentThread().getName());
//      String id = msg.body().toString();
//      if (id == null)
//        msg.reply("Dirty Input");
//      else {
//          mongo.findOne(COLLECTION_NAME, new JsonObject().put("id", Integer.parseInt(id)),new JsonObject(), results -> {
//            if(results.result()!=null){
//              Employee employee = new Employee(results.result());
//              msg.reply(Json.encodePrettily(employee));
//            }
//            else
//              msg.reply("Document not present");
//          });
//      }
//    });

    //update one
    //update one
    vertx.eventBus().consumer("update-one-address",msg->{
      System.out.println("Update mai ayya: "+Thread.currentThread().getName());
      JsonObject jsonObject = JsonObject.mapFrom(msg.body());
//      int id = jsonObject.getInteger("id");
      String id = jsonObject.getString("_id");
      mongo.findOneAndUpdate(COLLECTION_NAME, new JsonObject().put("_id",id),new JsonObject().put("$set", jsonObject),
        result -> {
          if (result.result()==null)
            msg.reply("Document not present");
          else {
            Employee employee = new Employee(jsonObject);
            msg.reply(Json.encodePrettily(jsonObject));
          }
        });
    });

    //delete one
    vertx.eventBus().consumer("delete-one-method",msg ->{
      System.out.println("Delete mai ayya: "+Thread.currentThread().getName());
//      int id = Integer.parseInt(msg.body().toString());
      String id = msg.body().toString();
      mongo.removeDocument(COLLECTION_NAME, new JsonObject().put("_id", id),result -> {
          if(result.result()==null || result.result().getRemovedCount()==0)
            msg.reply("Document not present");
          else
            msg.reply("Document deleted Successfully!");
      });

    });

    //delete all
    vertx.eventBus().consumer("delete-all-address",msg->{
      System.out.println("Delete all mai ayya: "+Thread.currentThread().getName());
      final int[] max_cnt;
      mongo.removeDocuments(COLLECTION_NAME,new JsonObject(),result -> {
          if(result.result().getRemovedCount()!=0)
            msg.reply("All Document deleted Successfully!");
      });
    });




/* Original Code Starting
    //get all
    vertx.eventBus().consumer("get-all-address",msg->{
      System.out.println("Get all mai ayya");
      //
      for(int i =0;i< 100000;i++) System.out.println(" "+i);
      vertx.setPeriodic(10000, id -> {
        // This handler will get called every second
        System.out.println("timer fired!");
      });
      //
      msg.reply(Json.encodePrettily(EmpObj.products));
    });

    //get one
    vertx.eventBus().consumer("get-one-address",msg->{
      System.out.println("Get one mai ayya");
      String id = msg.body().toString();
      if (id == null)
        msg.reply("Dirty Input");
      else {
        if(Integer.parseInt(id)>=Employee.getMaxId())
          msg.reply("Id not present");
        else {
          final Integer idAsInteger = Integer.valueOf(id);
          Employee employee = EmpObj.products.get(idAsInteger);
          msg.reply(Json.encodePrettily(employee));
        }
      }
    });

    //update one
    vertx.eventBus().consumer("update-one-address",msg->{
      System.out.println("Update mai ayya");
      JsonObject jsonObject = JsonObject.mapFrom(msg.body());
      Employee employee = EmpObj.products.get(Integer.parseInt(jsonObject.getString("id")));
      if (employee == null)
        msg.reply("Id not present");
      else {
        employee.setName(jsonObject.getString("name"));
        employee.setAge(jsonObject.getString("age"));
        msg.reply(Json.encodePrettily(employee));
      }
    });

    //delete one
    vertx.eventBus().consumer("delete-one-method",msg ->{
      System.out.println("Delete mai ayya");
      String id = msg.body().toString();
      if (id == null)
        msg.fail(400,"Error");
      else {
        if(Integer.parseInt(id)>=Employee.getMaxId())
          msg.reply("Id not present");
        else{
          EmpObj.products.remove(Integer.parseInt(id));
          msg.reply("Removed Successfully");
        }
      }

    });

    //delete all
    vertx.eventBus().consumer("delete-all-address",msg->{
      System.out.println("Delete all mai ayya");
      EmpObj.products.clear();
      msg.reply("Deleted all documents");
    });

 Original Code End */
  }

//  private void createSomeData() {
//    System.out.println("Create mai ayya");
//    Employee employee1 = new Employee("Tejas", "22");
//    products.put(employee1.getId(), employee1);
//    Employee employee2 = new Employee("Ojas", "11");
//    products.put(employee2.getId(), employee2);
//    Employee employee3 = new Employee("Vaibhav", "18");
//    products.put(employee3.getId(), employee3);
//  }

}

package com.example.vertx_tb_demothread;

import io.vertx.core.*;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.StaticHandler;

public class MainVerticle extends AbstractVerticle {

  public static final String COLLECTION_NAME = "Employees";
  MongoClient mongo;
  Router router = Router.router(vertx);
  @Override
  public void start(Promise<Void> future) throws Exception {
    mongo = MongoClient.createShared(vertx,config());
    DeploymentOptions options = new DeploymentOptions();
    options.setWorker(true).setInstances(8);
    vertx.deployVerticle(WorkerVerticle.class.getName(),options);
    vertx.deployVerticle(new MethodVerticle());
//    createSomeData();
    createSomeData((nothing) -> startWebApp(), future);

  }

  //new mothod
  private void createSomeData(Handler<AsyncResult<Void>> next, Promise<Void> future) {
    System.out.println("Create mai ayya");
    Employee employee1 = new Employee("Tejas", "22", "APT-Siddhi","31/05/2021");
//    EmpObj.products.put(employee1.getId(), employee1);
    Employee employee2 = new Employee("Ojas", "21","APT-Siddhi","31/05/2021");
//    EmpObj.products.put(employee2.getId(), employee2);
    Employee employee3 = new Employee("Vaibhav", "20","APT-Siddhi","31/05/2021");
//    EmpObj.products.put(employee3.getId(), employee3);

    mongo.count(COLLECTION_NAME, new JsonObject(), count -> {
      if (count.succeeded()) {
        System.out.println("Sucessful to get count "+count.result());
        if (count.result() == 0) {
          // no whiskies, insert data
          mongo.insert(COLLECTION_NAME, employee1.toJson(), ar -> {
            if (ar.failed()) {
              System.out.println("Failed to Insert Employee 1");
              future.fail("Failed to Insert Employee 1");
            } else {
              System.out.println("Sucessful to Insert Employee 1");
              mongo.insert(COLLECTION_NAME, employee2.toJson(), ar2 -> {
                if (ar2.failed()) {
                  System.out.println("Failed to Insert Employee 2");
                  future.fail("Failed to Insert Employee 2");
                } else {
                  System.out.println("Sucessful to Insert Employee 2");
                  mongo.insert(COLLECTION_NAME, employee3.toJson(), ar3 -> {
                    if (ar3.failed()) {
                      System.out.println("Failed to Insert Employee 3");
                      future.fail("Failed to Insert Employee 3");
                    } else {
                      System.out.println("Sucessful to Insert Employee 3");
                      next.handle(Future.succeededFuture());
                    }
                  });
                }
              });
            }
          });
        } else {
          next.handle(Future.succeededFuture());
        }
      } else {
        // report the error
        future.fail("Failed to Insert Employee 3");
        System.out.println("Failed to provide Service by MongoDB");
      }
    });
  }

  private void startWebApp() {
    System.out.println("Router added");
    // Create a router object.
    router.route("/").handler(routingContext -> {
      HttpServerResponse response = routingContext.response();
      response
        .putHeader("content-type", "text/html")
        .end("<h1>Hello from my first Vert.x 3 application</h1>");
    });

    router.route("/public/*").handler(StaticHandler.create("public"));
    router.route("/api/Employee*").handler(BodyHandler.create());
    router.post("/api/Employee").handler(this::postOne);
    router.get("/api/Employee").handler(this::getAll);
    router.get("/api/Employee/:id").handler(this::getOne);
    router.put("/api/Employee/:id").handler(this::updateOne);
    router.delete("/api/Employee/:id").handler(this::deleteOne);
    router.delete("/api/Employee").handler(this::deleteAll);

    vertx
      .createHttpServer()
      .requestHandler(router)
      .listen(
        // Retrieve the port from the configuration,
        // default to 8080.
        config().getInteger("http.port", 9050)
      );
    System.out.println("Server started at: "+ 9050);
  }

  //new mothods end

  private void deleteAll(RoutingContext routingContext) {
//    System.out.println("From Main Verticle: "+Thread.currentThread().getName());
    vertx.eventBus().request("delete-all-address","",reply->{
      routingContext.request().response()
        .setStatusCode(200)
        .end(String.valueOf(reply.result().body()));
    });
  }

  private void deleteOne(RoutingContext routingContext) {
//    System.out.println("From Main Verticle: "+Thread.currentThread().getName());
    String id = routingContext.request().getParam("id");      //contains _id of mongodb
    vertx.eventBus().request("delete-one-method",id,reply ->{
//      System.out.println("calling delete method");
      if(reply.failed()){
        routingContext.request().response()
          .end(String.valueOf(reply.result().body()));
      }
      else{
        routingContext.request().response()
          .setStatusCode(200)
          .putHeader("content-type", "plain/text; charset=utf-8")
          .end(String.valueOf(reply.result().body()));
      }
    });
  }

  private void updateOne(RoutingContext routingContext) {
//    System.out.println("From Main Verticle: "+Thread.currentThread().getName());
    final String id = routingContext.pathParam("id");             //contains _id of mongodb
    JsonObject jsonObj = routingContext.getBodyAsJson();
//    jsonObj.put("id",Integer.parseInt(id));
    jsonObj.put("_id",id);
    vertx.eventBus().request("update-one-address",jsonObj,reply ->{
      if(reply.failed()){
        routingContext.request().response()
          .end(String.valueOf(reply.result().body()));
      }
      else{
        routingContext.request().response()
          .setStatusCode(201)
          .putHeader("content-type", "application/json; charset=utf-8")
          .end(String.valueOf(reply.result().body()));
      }
    });
  }

  private void getOne(RoutingContext routingContext) {
//    System.out.println("From Main Verticle: "+Thread.currentThread().getName());
    final String id = routingContext.pathParam("id");               //contains _id of mongodb
    vertx.eventBus().request("get-one-address",id,reply ->{
      if(reply.failed()) {
        routingContext.request().response()
          .end(String.valueOf(reply.result().body()));
      }else{
        routingContext.request().response()
          .setStatusCode(200)
          .putHeader("content-type", "application/json; charset=utf-8")
          .end(String.valueOf(reply.result().body()));
      }
    });
  }

  private void getAll(RoutingContext routingContext) {
    System.out.println("From Main Verticle: "+Thread.currentThread().getName());
    vertx.eventBus().request("get-all-address","",reply->{
      routingContext.request().response()
        .setStatusCode(200)
        .end(String.valueOf(reply.result().body()));
    });
  }

  private void postOne(RoutingContext routingContext) {
//    System.out.println("From Main Verticle: "+Thread.currentThread().getName());
    JsonObject jsonObj = routingContext.getBodyAsJson();
    vertx.eventBus().request("post-address", jsonObj, reply->{
      routingContext.request().response()
      .end(String.valueOf(reply.result().body()));
    });
  }
//  private void createSomeData() {
//    System.out.println("Create mai ayya");
//    Employee employee1 = new Employee("Tejas", "22");
//    EmpObj.products.put(employee1.getId(), employee1);
//    Employee employee2 = new Employee("Ojas", "11");
//    EmpObj.products.put(employee2.getId(), employee2);
//    Employee employee3 = new Employee("Vaibhav", "18");
//    EmpObj.products.put(employee3.getId(), employee3);
//  }
}

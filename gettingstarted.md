---
layout: default
title: Getting Started
---

# Getting Started

The MemoNodes data store can be easily integrated in an existing Java project
useing the provided libraries.  
MemoNodes provides the following libraries for deploying data-oriented applications 
on different platforms, with different storage facilities.
The data store can be a file cached locally on your mobile Android device, or a file 
accessible from a regular application server (Tomcat, JBoss, Jetty) or other servlet containers (Winstone) that you host
yourself, or on Google App Engine in the cloud.

This tutorial shows how to create an application using MemoNodes, and 
deploy, run and test it on Google App Engine.
Google Datastore is used for persistency, and MemoNodes GAE implementation is used in this case.

Creating a project for another type of deployment is similar, you basically just 
have to include the MemoNodes libraries and configure a web servlet with the alternative library component used.

The tutorial contains the following steps:

- [Prerequisites](#prerequisites)
- [Project Setup](#project_setup)
- [Usage](#usage)
- [Deployment](#deployment)


## Prerequisites {#prerequisites}

This tutorial assumes you have installed Eclipse and the Google Web Toolkit plugin.

- Download and unzip [Eclipse Helios (3.6)](http://www.eclipse.org/helios/).
  On the site, click Download Helios, Eclipse IDE for Java Developers.
  Then select the correct zip file for your system and download it.
  Unzip it somewhere on your computer and start Eclipse.
  Note that you need to have a Java SDK installed on your computer.

- In Eclipse install the [Google Web Toolkit](http://code.google.com/webtoolkit/) plugin. 
  Go to menu Help, Install New Software... Click Add to add a new software source,
  and enter name "Google Web Toolkit" and location 
  [http://dl.google.com/eclipse/plugin/3.6](http://dl.google.com/eclipse/plugin/3.6).
  Click Ok. 
  Then, select and install "Google Plugin for Eclipse" and "SDKs".

Note that for a typical java web application you will need the 
[Web Tools Platform plugin](http://download.eclipse.org/webtools/repository/helios/) 
and a [Tomcat server](http://tomcat.apache.org/).


## Project Setup {#project_setup}

We will create a new project, add the required libraries, and configure a
web servlet as main entry point to access MemoNodes memory.

- Create a new GWT project in Eclipse via menu New, Project, Google,
  Web Application Project. Select a project name and a package name, 
  for example "MyMemoNodesProject" and "com.mycompany.myproject".
  Unselect the option "Use Google Web Toolkit", and select the options 
  "Use Google App Engine" and "Generate GWT project sample code" checked. 
  Click Finish.

- Download the following jar files, and put them in your Eclipse project
  in the folder war/WEB-INF/lib. 
  If you don't want to download all libraries individually, you can download the
  zip files *memonodes-core-bundle.zip* and *memonodes-gae-bundle.zip*
  containing all dependent libraries 
  [here](https://github.com/almende/MemoNodes/tree/master/java/bin/current).  

  - [memonodes-core.jar](https://github.com/almende/MemoNodes/tree/master/java/bin/current)
  
    - [json-lib-2.4-jdk15.jar](http://json-lib.sourceforge.net/)
    - [jakarta commons-lang 2.5](http://commons.apache.org/lang/)
    - [jakarta commons-beanutils 1.8.0](http://commons.apache.org/beanutils/)
    - [jakarta commons-collections 3.2.1](http://commons.apache.org/collections/)
    - [jakarta commons-logging 1.1.1](http://commons.apache.org/logging/)
    - [ezmorph 1.0.6](http://ezmorph.sourceforge.net/)

  - [memonodes-gae.jar](https://github.com/almende/MemoNodes/tree/master/java/bin/current)
  
    - [twig-persist-2.0-beta4.jar](http://code.google.com/p/twig-persist/)
    - [guava-11.0.2.jar](http://code.google.com/p/guava-libraries/)
  
- Right-click the added jars in Eclipse, and click Build Path, "Add to Build Path". 
    
- Now, you need to configure a web-servlet to include the servlet for accessing MemoNodes memory. 
  Open the file web.xml under war/WEB-INF. Insert the following lines
  inside the &lt;web-app&gt; tag:
  <pre><code>&lt;servlet&gt;
    &lt;servlet-name&gt;MemoNodesServlet&lt;/servlet-name&gt;
    &lt;servlet-class&gt;com.almende.chap.memo.memonodes.servlet.MemoNodesServlet&lt;/servlet-class&gt;
    &lt;init-param&gt;
      &lt;description&gt;The context for reading/writing persistent data&lt;/description&gt; 
      &lt;param-name&gt;context&lt;/param-name&gt;
      &lt;param-value&gt;com.almende.memo.memonodes.context.google.DatastoreContext&lt;/param-value&gt;
    &lt;/init-param&gt;
  &lt;/servlet&gt;
  &lt;servlet-mapping&gt;
    &lt;servlet-name&gt;MemoNodesServlet&lt;/servlet-name&gt;
    &lt;url-pattern&gt;/memonodes/*&lt;/url-pattern&gt;
  &lt;/servlet-mapping&gt;
  </code></pre>

  The configuration consists of a standard servlet and servlet mapping definition.
  The MemoNodesServlet needs one initialization parameter, called *context*. 
  It specifies the context that will be available for the 
  objects to read and write persistent data.

## Usage {#usage}

Now the project can be started, compiled and run, and you can see one of the examples in action.

- Start the project in Eclipse via menu Run, Run As, Web Application.
  
- To verify if the MemoNodesServlet is running, open your browser and
  go to URL http://localhost:8888/memonodes/.
  This should give a response *"Error: POST request containing a JSON-RPC 
  message expected"*.
  
- MemoNodes can be accessed via an HTTP POST request. 
  The body of this post request must contain a JSON-RPC message.
  To execute HTTP requests you can use a REST client like 
  [Postman](https://chrome.google.com/webstore/detail/fdmmgilgnpjigdojojpjoooidkmcomcm) in Chrome,
  [RESTClient](https://addons.mozilla.org/en-US/firefox/addon/restclient/?src=search) in Firefox,
  or with a tool like [cURL](http://curl.haxx.se/).

  Perform the following HTTP POST request to the MemoNodesServlet on the url
  <pre><code>http://localhost:8888/memonodes/MemoNodesServlet</code></pre>
  
  With request body:
  <pre><code>{
    "id": 1, 
    "method": "add_node",
    "params": {
      "node": "node1"
    }
  }</code></pre>
  
  This request will return the following response:
  <pre><code>{
    "jsonrpc": "2.0",
    "id": 1,
    "result": "node(node1)"
  }</code></pre>

## Deployment {#deployment}

Now you can deploy your application in the cloud, to Google App Engine.

- Register an application in appengine.
  In your browser, go to [https://appengine.google.com](https://appengine.google.com).
  You will need a Google account for that. Create a new application by clicking
  Create Application. Enter an identifier, for example "mymemonodes" and a 
  title and click Create Application.
  
- In Eclipse, go to menu Project, Properties. Go to the page Google, App Engine.
  Under *Deployment*, enter the identifier "mymemonodes" of your application 
  that you have just created on the appengine site. Set version to 1. Click Ok.

- In Eclipse, right-click your project in the Package Explorer. In the context
  menu, choose Google, Deploy to App Engine. Click Deploy in the opened window,
  and wait until the deployment is finished.
  
- Your application is now up and running and can be found at 
  http://mymemonodes.appspot.com (where you have to replace the identifier with 
  your own). The MemoNodes data store you have created is accessable at
  http://mymemonodes.appspot.com/memonodes/MemoNodesServlet.

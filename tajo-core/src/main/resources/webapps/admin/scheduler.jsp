<%
  /*
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements. See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership. The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
%>
<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>

<%@ page import="org.apache.tajo.master.TajoMaster" %>
<%@ page import="org.apache.tajo.master.querymaster.QueryJobManager" %>
<%@ page import="org.apache.tajo.scheduler.Scheduler" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>

<%
  TajoMaster master = (TajoMaster) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
  TajoMaster.MasterContext masterContext = master.getContext();
  QueryJobManager queryJobManager = masterContext.getQueryJobManager();

  Scheduler scheduler = queryJobManager.getScheduler();
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
  <link rel="stylesheet" type = "text/css" href = "/static/style.css" />
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <title>Tajo</title>
  <script src="/static/js/jquery.js" type="text/javascript"></script>
  <script type="text/javascript">
    function killQuery(queryId) {
        if (!confirm("Are you sure removing a query: " + queryId + " ?")) {
            return;
        }
        $.ajax({
            type: "POST",
            url: "query_exec",
            data: { action: "killQuery", queryId: queryId }
        })
        .done(function(msg) {
            var resultJson = $.parseJSON(msg);
            if(resultJson.success == "false") {
                alert(resultJson.errorMessage);
            } else {
                alert(resultJson.successMessage);
                location.reload();
            }
        })
    }
  </script>
</head>
<body>
<%@ include file="header.jsp"%>
<div class='contents'>
  <h2>Tajo Master: <%=master.getMasterName()%></h2>
  <hr/>
  <h3>Scheduler: <%=scheduler.getClass().getName()%></h3>
  <%=scheduler.getStatusHtml()%>
</div>
</body>
</html>
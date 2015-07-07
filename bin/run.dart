import "dart:async";
import "dart:convert";

import "package:rethinkdb_driver/rethinkdb_driver.dart" as Rethink;
import "package:dslink/dslink.dart";
import "package:dslink/nodes.dart";

LinkProvider link;
final Rethink.Rethinkdb r = new Rethink.Rethinkdb();
Map<String, Rethink.Connection> conns = {};

main(List<String> args) async {
  link = new LinkProvider(
    args,
    "RethinkDB-",
    defaultNodes: {
      "Create_Connection": {
        r"$name": "Create Connection",
        r"$is": "createConnection",
        r"$invokable": "write",
        r"$params": [
          {
            "name": "name",
            "type": "string"
          },
          {
            "name": "host",
            "type": "string"
          },
          {
            "name": "port",
            "type": "uint"
          },
          {
            "name": "auth",
            "type": "string"
          }
        ]
      }
    },
    profiles: {
      "connection": (String path) => new ConnectionNode(path),
      "createConnection": (String path) => new CreateConnectionNode(path),
      "deleteConnection": (String path) => new DeleteConnectionNode(path),
      "editConnection": (String path) => new SimpleActionNode(path, (Map<String, dynamic> params) async {
        var name = params["name"];
        var oldName = path.split("/")[1];
        ConnectionNode conn = link["/${oldName}"];
        if (name != null && name != oldName) {
          if ((link.provider as SimpleNodeProvider).nodes.containsKey("/${name}")) {
            return {
              "success": false,
              "message": "Connection '${name}' already exists."
            };
          } else {
            var n = conn.serialize(false);
            link.removeNode("/${oldName}");
            link.addNode("/${name}", n);
            (link.provider as SimpleNodeProvider).nodes.remove("/${oldName}");
            conn = link["/${name}"];
          }
        }

        link.save();

        var host = params["host"];
        var oldHost = conn.configs[r"$$rethink_host"];

        if (host != null && oldHost != host) {
          conn.configs[r"$$rethink_host"] = host;
          try {
            await conn.setup();
          } catch (e) {
            return {
              "success": false,
              "message": "Failed to connect to database: ${e}"
            };
          }
        }

        link.save();

        return {
          "success": true,
          "message": "Sucess"
        };
      }),
      "createDatabase": (String path) => new SimpleActionNode(path, (Map<String, dynamic> params) async {
        var dbName = params["name"];
        var conn = conns[new Path(new Path(path).parentPath).name];
        try {
          await r.dbCreate(dbName).run(conn);
          return {
            "success": true,
            "message": "Success!"
          };
        } catch (e) {
          return {
            "success": false,
            "message": e
          };
        }
      }),
      "dropDatabase": (String path) => new SimpleActionNode(path, (Map<String, dynamic> params) async {
        var dbName = params["name"];
        var conn = conns[new Path(new Path(path).parentPath).name];
        try {
          await r.dbDrop(dbName).run(conn);
          return {
            "success": true,
            "message": "Success!"
          };
        } catch (e) {
          return {
            "success": false,
            "message": e
          };
        }
      }),
      "listDatabases": (String path) => new SimpleActionNode(path, (Map<String, dynamic> params) async {
        var conn = conns[new Path(new Path(path).parentPath).name];
        return (await r.dbList().run(conn)).map((x) => [x]);
      }),
      "createTable": (String path) => new SimpleActionNode(path, (Map<String, dynamic> params) async {
        var dbName = params["db"];
        var tableName = params["name"];
        var conn = conns[new Path(new Path(path).parentPath).name];
        try {
          await r.db(dbName).tableCreate(tableName).run(conn);
          return {
            "success": true,
            "message": "Success!"
          };
        } catch (e) {
          return {
            "success": false,
            "message": e
          };
        }
      }),
      "dropTable": (String path) => new SimpleActionNode(path, (Map<String, dynamic> params) async {
        var dbName = params["db"];
        var tableName = params["name"];
        var conn = conns[new Path(new Path(path).parentPath).name];
        try {
          await r.db(dbName).tableDrop(tableName).run(conn);
          return {
            "success": true,
            "message": "Success!"
          };
        } catch (e) {
          return {
            "success": false,
            "message": e
          };
        }
      }),
      "listTables": (String path) => new SimpleActionNode(path, (Map<String, dynamic> params) async {
        var dbName = params["db"];
        var conn = conns[new Path(new Path(path).parentPath).name];
        return (await r.db(dbName).tableList().run(conn)).map((x) => [x]);
      }),
      "insertDocument": (String path) => new SimpleActionNode(path, (Map<String, dynamic> params) async {
        var dbName = params["db"];
        var tableName = params["table"];
        var conn = conns[new Path(new Path(path).parentPath).name];
        var objParam = params["obj"];
        var obj;
        if (objParam is String) {
          try {
            obj = JSON.decode(objParam);
          } catch (e) {
            return {
              "sucess": false,
              "message": e
            };
          }
        } else if (objParam is Map) {
          obj = objParam;
        } else {
          return {
            "success": false,
            "message": "Unknown type"
          };
        }
        try {
          await r.db(dbName).table(tableName).insert(obj).run(conn);
          return {
            "success": true,
            "message": "Success!"
          };
        } catch (e) {
          return {
            "success": false,
            "message": e
          };
        }
      }),
      "queryData": (String path) => new QueryDataNode(path, link.provider)
    },
    autoInitialize: false,
    encodePrettyJson: true
  );

  link.init();
  link.connect();
  link.save();
}

class CreateConnectionNode extends SimpleNode {
  CreateConnectionNode(String path) : super(path);

  @override
  onInvoke(Map<String, dynamic> params) async {
    link.addNode("/${params["name"]}", {
      r"$is": "connection",
      r"$$rethink_host": params["host"],
      r"$$rethink_port": params["port"],
      r"$$rethink_auth": params["auth"]
    });

    link.save();

    return {};
  }
}

class ConnectionNode extends SimpleNode {
  ConnectionNode(String path) : super(path);

  @override
  void onCreated() {
    setup();
  }

  setup() async {
    var name = new Path(path).name;

    if (conns.containsKey(name)) {
      await conns[name].close();
      conns.remove(name);
    }

    var host = configs[r"$$rethink_host"];
    var port = int.parse(configs[r"$$rethink_port"]);
    //var auth = configs[r"$$rethink_auth"];
    r.connect(host: host, port: port).then((conn) async {
      conns[name] = conn;

      var x = {
        "Create_Database": {
          r"$name": "Create Database",
          r"$is": "createDatabase",
          r"$invokable": "write",
          r"$results": "values",
          r"$params": [
            {
              "name": "name",
              "type": "string"
            }
          ],
          r"$columns": [
            {
              "name": "success",
              "type": "bool"
            },
            {
              "name": "message",
              "type": "string"
            }
          ]
        },
        "List_Databases": {
          r"$name": "List Databases",
          r"$is": "listDatabases",
          r"$invokable": "write",
          r"$result": "table",
          r"$columns": [
            {
              "name": "name",
              "type": "string"
            }
          ],
          r"$params": []
        },
        "Drop_Database": {
          r"$name": "Drop Database",
          r"$is": "dropDatabase",
          r"$invokable": "write",
          r"$results": "values",
          r"$params": [
            {
              "name": "name",
              "type": "string"
            }
          ],
          r"$columns": [
            {
              "name": "success",
              "type": "bool"
            },
            {
              "name": "message",
              "type": "string"
            }
          ]
        },
        "Create_Table": {
          r"$name": "Create Table",
          r"$is": "createTable",
          r"$invokable": "write",
          r"$results": "values",
          r"$params": [
            {
              "name": "db",
              "type": "string"
            },
            {
              "name": "name",
              "type": "string"
            }
          ],
          r"$columns": [
            {
              "name": "success",
              "type": "bool"
            },
            {
              "name": "message",
              "type": "string"
            }
          ]
        },
        "Drop_Table": {
          r"$name": "Drop Table",
          r"$is": "dropTable",
          r"$invokable": "write",
          r"$results": "values",
          r"$params": [
            {
              "name": "db",
              "type": "string"
            },
            {
              "name": "name",
              "type": "string"
            }
          ],
          r"$columns": [
            {
              "name": "success",
              "type": "bool"
            },
            {
              "name": "message",
              "type": "string"
            }
          ]
        },
        "List Tables": {
          r"$name": "List Tables",
          r"$is": "listTables",
          r"$invokable": "write",
          r"$result": "table",
          r"$params": [
            {
              "name": "db",
              "type": "string"
            }
          ],
          r"$columns": [
            {
              "name": "name",
              "type": "string"
            }
          ]
        },
        "Query_Data": {
          r"$name": "Query Data",
          r"$is": "queryData",
          r"$invokable": "read",
          r"$params": [
            {
              "name": "db",
              "type": "string"
            },
            {
              "name": "table",
              "type": "string"
            }
          ],
          r"$result": "table"
        },
        "Insert_Document": {
          r"$name": "Insert Document",
          r"$is": "insertDocument",
          r"$invokable": "write",
          r"$params": [
            {
              "name": "db",
              "type": "string"
            },
            {
              "name": "table",
              "type": "string"
            },
            {
              "name": "obj",
              "type": "dynamic"
            }
          ],
          r"$columns": [
            {
              "name": "success",
              "type": "bool"
            },
            {
              "name": "message",
              "type": "string"
            }
          ]
        },
        "Edit_Connection": {
          r"$name": "Edit Connection",
          r"$is": "editConnection",
          r"$invokable": "write",
          r"$params": [
            {
              "name": "name",
              "type": "string",
              "default": name
            },
            {
              "name": "host",
              "type": "string",
              "default": host
            },
            {
              "name": "port",
              "type": "uint",
              "default": port
            }
          ],
          r"$columns": [
            {
              "name": "success",
              "type": "bool"
            },
            {
              "name": "message",
              "type": "string"
            }
          ]
        },
        "Delete_Connection": {
          r"$name": "Delete Connection",
          r"$is": "deleteConnection",
          r"$invokable": "write",
          r"$result": "values",
          r"$params": [],
          r"$columns": []
        }
      };

      for (var a in x.keys) {
        link.removeNode("${path}/${a}");
        link.addNode("${path}/${a}", x[a]);
      }
    });
  }
}

class DeleteConnectionNode extends SimpleNode {
  DeleteConnectionNode(String path) : super(path);

  @override
  onInvoke(Map<String, dynamic> params) {
    link.removeNode(new Path(path).parentPath);
    link.save();
    return {};
  }
}

class QueryDataNode extends SimpleNode {
  QueryDataNode(String path, NodeProvider provider) : super(path, provider);

  @override
  onInvoke(Map<String, dynamic> params) {
    var tr = new AsyncTableResult();

    new Future(() async {
      var dbName = params["db"];
      var tableName = params["table"];
      var conn = conns[new Path(new Path(path).parentPath).name];
      Rethink.Cursor cursor = await r.db(dbName).table(tableName).run(conn);
      var data = [];
      var keys = new Set<String>();
      await cursor.listen((x) {
        if (x is Map) {
          data.add(x);
          keys.addAll(x.keys);
        }
      }).asFuture();
      var col = keys.map((x) =>  {
        "name": x,
        "type": "dynamic"
      }).toList();
      tr.columns = col;
      tr.update(data, StreamStatus.closed);
    });

    return tr;
  }
}

import "dart:async";
import "dart:convert";

import "package:rethinkdb_driver/rethinkdb_driver.dart" as Rethink;
import "package:dslink/dslink.dart";
import "package:dslink/nodes.dart";

LinkProvider link;
final Rethink.Rethinkdb r = new Rethink.Rethinkdb();

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
      "createDatabase": (String path) => new CreateDatabaseNode(path),
      "deleteDatabase": (String path) => new DeleteDatabaseNode(path),
      "database": (String path) => new DatabaseNode(path),
      "createTable": (String path) => new CreateTableNode(path),
      "deleteTable": (String path) => new DeleteTableNode(path),
      "table": (String path) => new TableNode(path),
      "insert": (String path) => new SimpleActionNode(path, (Map<String, dynamic> params) {
        var p = new Path(path);
        var tableNode = new Path(p.parentPath);
        var dbNode = new Path(tableNode.parentPath);
        var tableName = tableNode.name;
        var dbName = dbNode.name;
        var conn = new Path(dbNode.parentPath).name;
        var objParam = params["object"];
        var obj;
        if (objParam is String) {
          try {
            obj = JSON.decode(obj);
          } catch (e) {
            return {
              "message": e
            };
          }
        } else if (objParam is Map) {
          obj = objParam;
        } else {
          return {
            "message": "Unknown type"
          };
        }
        r.db(dbName).table(tableName).insert(obj).run(conns[conn]);
      })
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
          r"$params": [
            {
              "name": "name",
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

      List<String> dbs = await r.dbList().run(conn);
      for (var db in dbs) {
        var dbPath = "${path}/${db}";
        var dbn = new DatabaseNode(dbPath);
        await dbn.setup();
        x[db] = dbn.children;
      }

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

class CreateDatabaseNode extends SimpleNode {
  CreateDatabaseNode(String path) : super(path);

  @override
  onInvoke(Map<String, dynamic> params) async {
    var p = new Path(path).parentPath;
    link.addNode("$p/${params["name"]}", {
      r"$is": "database"
    });

    link.save();

    return {};
  }
}

class DeleteDatabaseNode extends SimpleNode {
  DeleteDatabaseNode(String path) : super(path);

  @override
  onInvoke(Map<String, dynamic> params) async {
    var dbNode = new Path(new Path(path).parentPath);
    var dbName = dbNode.name;
    var conn = new Path(dbNode.parentPath).name;

    // Remove database from RethinkDB
    await r.dbDrop(dbName).run(conns[conn]);

    // Remove the node
    link.removeNode(new Path(path).parentPath);
    link.save();
    return {};
  }
}

class DatabaseNode extends SimpleNode {
  DatabaseNode(String path) : super(path);

  @override
  void onCreated() {
    setup();
  }

  setup() async {
    var p = new Path(path);
    var connName = new Path(p.parentPath).name;
    var dbName = p.name;
    await new Future.delayed(new Duration(seconds: 1));
    var conn = conns[connName];
    await r.dbCreate(dbName).run(conn).catchError((e) {
      print("DB: ${e.message}");
    }).then((_) async {
      var x = {
        "Delete_Database": {
          r"$name": "Delete Database",
          r"$is": "deleteDatabase",
          r"$invokable": "write",
          r"$params": []
        },
        "Create_Table": {
          r"$name": "Create Table",
          r"$is": "createTable",
          r"$invokable": "write",
          r"$params": [
            {
              "name": "name",
              "type": "string"
            }
          ]
        }
      };

      var tables = await r.db(dbName).tableList().run(conn);
      for (var table in tables) {
        var tablePath = "${path}/${table}";
        var tableNode = new TableNode(tablePath);
        await tableNode.setup();
        x[table] = tableNode.children;
      }

      for (var a in x.keys) {
        link.removeNode("${path}/${a}");
        link.addNode("${path}/${a}", x[a]);
      }
    });
  }
}

class CreateTableNode extends SimpleNode {
  CreateTableNode(String path) : super(path);

  @override
  onInvoke(Map<String, dynamic> params) async {
    var p = new Path(path).parentPath;
    link.addNode("$p/${params["name"]}", {
      r"$is": "table"
    });

    link.save();

    return {};
  }
}

class DeleteTableNode extends SimpleNode {
  DeleteTableNode(String path) : super(path);

  @override
  onInvoke(Map<String, dynamic> params) async {
    var p = new Path(path);
    var tableNode = new Path(p.parentPath);
    var dbNode = new Path(tableNode.parentPath);
    var tableName = tableNode.name;
    var dbName = dbNode.name;
    var conn = new Path(dbNode.parentPath).name;

    // Remove table from RethinkDB
    await r.db(dbName).tableDrop(tableName).run(conns[conn]);

    // Remove the node
    link.removeNode(new Path(path).parentPath);
    link.save();
    return {};
  }
}

class TableNode extends SimpleNode {
  TableNode(String path) : super(path);

  @override
  void onCreated() {
    setup();
  }

  setup() async {
    var p = new Path(path);
    var dbNode = new Path(p.parentPath);
    var dbName = dbNode.name;
    var connName = new Path(dbNode.parentPath).name;
    var tableName = p.name;
    var conn = conns[connName];
    try {
      await r.db(dbName).tableCreate(tableName).run(conn);
    } catch (e) {
      print("Table: ${e.message}");
    }

    var x = {
      "Delete_Table": {
        r"$name": "Delete Table",
        r"$is": "deleteTable",
        r"$invokable": "write",
        r"$params": []
      },
      "Insert": {
        r"$name": "Insert",
        r"$is": "insert",
        r"$invokable": "write",
        r"$results": "values",
        r"$params": [
          {
            "name": "object",
            "type": "dynamic"
          }
        ]
      }
    };

    for (var a in x.keys) {
      link.removeNode("${path}/${a}");
      link.addNode("${path}/${a}", x[a]);
    }
  }
}

Map<String, Rethink.Connection> conns = {};

String dbFromPath(String path) => path.split("/").take(2)[1];
String connFromPath(String path) => path.split("/").take(2).first;

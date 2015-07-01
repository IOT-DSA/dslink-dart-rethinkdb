import "dart:async";

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
      "createConnection": (String path) => new CreateConnectionNode(path),
      "listTables": (String path) => new SimpleActionNode(path, (Map<String, dynamic> params) async {
        var db = dbFromPath(path);
        var conn = connFromPath(path);
        print(db);
        print(conn);
        //return (await r.db(db).tableList().run(conn));
      }),
      "createDatabase": (String path) => new CreateDatabaseNode(path),
      "database": (String path) => new DatabaseNode(path)
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
    Rethink.Connection conn = await r.connect(host: host, port: port);
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

    for (var a in x.keys) {
      link.removeNode("${path}/${a}");
      link.addNode("${path}/${a}", x[a]);
    }
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
    print(p);
    link.addNode("$p/${params["name"]}", {
      r"$is": "database"
    });

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

    try {
      await new Future.delayed(new Duration(seconds: 1));
      var conn = conns[connName];
      print(dbName);
      await r.dbCreate(dbName).run(conn).catchError((err) {
        print(err);
      });
    } catch (e) {
      // Ignore
      if (e is Rethink.RqlCompileError) {
        //print(e.message);
        //print(e.frames);
      }
    }

    var x = {
      "List_Tables": {
        r"$name": "List Tables",
        r"$is": "listTables",
        r"$invokable": "write",
        r"$params": [
          {
            "name": "name",
            "type": "string"
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

import net from "net";
import {
  expiry_checker,
  writeToConnection,
  parseMultipleCommands,
  getCommandByteSize,
} from "./utils/utils.js";
import {
  redisKeyValuePair,
  redisList,
  blpopConnections,
  redisStream,
  blocked_streams,
  REPLICATABLE_COMMANDS,
  replicas_connected,
} from "./state/store.js";
import {
  lrange_handler,
  lpop_handler,
  blop_handler,
  rpush_handler,
  incr_handler,
} from "./handlers/lists.js";
import {
  generateStreamId,
  xadd_handler,
  x_range_handler,
  xread_handler,
} from "./handlers/streams.js";
import { MyQueue } from "./utils/queue.js";
import {
  multi_handler,
  exec_hanlder,
  discard_handler,
} from "./handlers/scheduler.js";
import { createMasterConnection } from "./handlers/master_connector.js";
import {
  master_handler,
  command_propogator,
} from "./handlers/master_handler.js";
import { serverConfig } from "./config.js";
console.log("Logs from your program will appear here!");
if (
  serverConfig.master_host !== undefined &&
  serverConfig.master_port !== undefined
) {
  console.log(
    `Replica mode: Connecting to master at ${serverConfig.master_host}:${serverConfig.master_port}`
  );
  createMasterConnection();
}
const server = net.createServer((connection) => {
  let taskqueue = new MyQueue();
  let multi = { active: false };
  let isMasterConnection = false;

  connection.on("data", (data) => {
    const commands = parseMultipleCommands(data);

    if (commands.length > 1) {
      commands.forEach((cmd) =>
        processCommand(cmd, connection, taskqueue, multi, data)
      );
      return;
    }

    const command = data.toString().split("\r\n");
    processCommand(command, connection, taskqueue, multi, data);
  });

  function processCommand(command, connection, taskqueue, multi, originalData) {
    const intr = command[2]?.toLowerCase();
    const intru = command[2]?.toUpperCase();
    
    if (intr == "replconf" && serverConfig.role == "slave") {
      if (command[4]?.toLowerCase() === "getack") {
        const response = `*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$${serverConfig.replica_offset.toString().length}\r\n${serverConfig.replica_offset}\r\n`;
        connection.write(response);
        isMasterConnection = true;
      }
      serverConfig.replica_offset += getCommandByteSize(originalData);
    } else if (intr == "replconf" && serverConfig.role == "master") {
      connection.write(`+OK\r\n`);
    } else if (intr == "psync" && serverConfig.role == "master") {
      master_handler(command, serverConfig.master_replica_connection);
    } else if (multi.active && intr != "exec" && intr != "discard") {
      multi_handler(originalData, connection, taskqueue);
    } else if (intr === "ping") {
      if (serverConfig.role == "master") {
        serverConfig.master_replica_connection = connection;
        replicas_connected.add(serverConfig.master_replica_connection);
        connection.write(`+PONG\r\n`);
      } else if (serverConfig.role == "slave") {
        isMasterConnection = true;
      } else {
        connection.write(`+PONG\r\n`);
      }
    } else if (intr === "echo") {
      connection.write(command[3] + "\r\n" + command[4] + "\r\n");
    } else if (intr === "set") {
      redisKeyValuePair.set(command[4], command[6]);
      if (command.length > 8) {
        expiry_checker(command, redisKeyValuePair);
      }

      if (serverConfig.role == "master") {
        console.log("calling propagator");
        command_propogator(command, originalData);
      }
      writeToConnection(
        connection,
        `+OK\r\n`,
        intr,
        serverConfig,
        REPLICATABLE_COMMANDS
      );
    } else if (intr === "get") {
      const value = redisKeyValuePair.get(command[4]);
      if (value === "ille_pille_kille" || value === undefined) {
        connection.write(`$-1\r\n`);
      } else {
        connection.write(`$${value.length}\r\n${value}\r\n`);
      }
    } else if (intr === "rpush") {
      rpush_handler(
        command,
        redisList,
        blpopConnections,
        connection,
        serverConfig
      );
    } else if (intr === "lrange") {
      lrange_handler(command, redisList, connection);
    } else if (intr === "lpush") {
      const key = command[4];
      if (!redisList[key]) {
        redisList[key] = [];
      }
      for (let i = 6; i < command.length; i += 2) {
        if (command[i]) redisList[key].unshift(command[i]);
      }
      writeToConnection(
        connection,
        `:${redisList[key].length}\r\n`,
        intr,
        serverConfig,
        REPLICATABLE_COMMANDS
      );
    } else if (intr === "llen") {
      const key = command[4];
      const len = redisList[key]?.length ?? 0;
      connection.write(`:${len}\r\n`);
    } else if (intr === "lpop") {
      lpop_handler(command, redisList, connection, serverConfig);
    } else if (intr === "blpop") {
      blop_handler(
        command,
        redisList,
        blpopConnections,
        connection,
        serverConfig
      );
    } else if (intr === "type") {
      const key = command[4];
      if (redisStream[key]) {
        connection.write("+stream\r\n");
      } else if (redisList[key]) {
        connection.write("+list\r\n");
      } else if (redisKeyValuePair.has(key)) {
        connection.write("+string\r\n");
      } else {
        connection.write("+none\r\n");
      }
    } else if (intr === "xadd") {
      xadd_handler(command, connection, blocked_streams, serverConfig);
    } else if (intr === "xrange") {
      x_range_handler(command[6], command[8], command, connection);
    } else if (intr === "xread") {
      xread_handler(command, connection, blocked_streams);
    } else if (intr == "incr") {
      incr_handler(command, redisKeyValuePair, connection, serverConfig);
    } else if (intr == "multi") {
      multi.active = true;
      connection.write(`+OK\r\n`);
    } else if (intr == "exec") {
      exec_hanlder(originalData, connection, taskqueue, multi);
    } else if (intr == "discard") {
      discard_handler(originalData, connection, taskqueue, multi);
    } else if (intr == "info") {
      let tmp_res = "role" + ":" + serverConfig.role + "\r\n";
      tmp_res += "master_replid" + ":" + serverConfig.master_replid + "\r\n";
      tmp_res += "master_repl_offset" + ":" + serverConfig.master_repl_offset;
      connection.write(`$${tmp_res.length}\r\n${tmp_res}\r\n`);
    } else {
      connection.write("-ERR unknown command\r\n");
    }
    
    if (serverConfig.role === "slave" && intr !== "replconf") {
      if (isMasterConnection || REPLICATABLE_COMMANDS.includes(intru)) {
        serverConfig.replica_offset += getCommandByteSize(originalData);
      }
    }
  }
});
server.listen(serverConfig.port, "127.0.0.1", () => {
  console.log(`server running on ${serverConfig.port}`);
});

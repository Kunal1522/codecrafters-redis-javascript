import net from "net";
import {
  expiry_checker,
  writeToConnection,
  parseMultipleCommands,
} from "./utils/utils.js";
import {
  redisKeyValuePair,
  redisList,
  blpopConnections,
  redisStream,
  blocked_streams,
  REPLICATABLE_COMMANDS,
  replicas_connected,
  pendingWaitRequest,
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
  wait_handler
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

  connection.on("data", (data) => {
    const commands = parseMultipleCommands(data);

    commands.forEach((cmd) =>
      processCommand(cmd, connection, taskqueue, multi, data)
    );
  });

  function processCommand(command, connection, taskqueue, multi, originalData) {
    const intr = command[0]?.toLowerCase();
    const intru = command[0]?.toUpperCase();
    
    if (intr=='replconf' && command[1]?.toLowerCase() === 'getack') {
        const getAckBytes = 37;
        const offsetBeforeGetAck = serverConfig.replica_offset - getAckBytes;
        const response = `*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$${offsetBeforeGetAck.toString().length}\r\n${offsetBeforeGetAck}\r\n`;
        if (serverConfig.master_replica_connection) {
          serverConfig.master_replica_connection.write(response);
        }
    } else if (intr == "replconf" && command[1]?.toLowerCase() === 'ack' && serverConfig.role == "master") {
      const replicaOffset = parseInt(command[2], 10);
      
      if (pendingWaitRequest.active) {
        if (replicaOffset >= pendingWaitRequest.expectedOffset) {
          if (!pendingWaitRequest.ackedReplicas.has(connection)) {
            pendingWaitRequest.ackedReplicas.add(connection);
            
            const ackedCount = pendingWaitRequest.ackedReplicas.size;
            if (ackedCount >= pendingWaitRequest.numRequired || ackedCount >= replicas_connected.size) {
              if (pendingWaitRequest.timeoutId) {
                clearTimeout(pendingWaitRequest.timeoutId);
              }
              pendingWaitRequest.clientConnection.write(`:${ackedCount}\r\n`);
              pendingWaitRequest.active = false;
              pendingWaitRequest.ackedReplicas.clear();
            }
          }
        }
      }
    } else if (intr == "replconf" && serverConfig.role == "master") {
      connection.write(`+OK\r\n`);
    } else if (intr == "psync" && serverConfig.role == "master") {
      master_handler(command, serverConfig.master_replica_connection);
    } else if (multi.active && intr != "exec" && intr != "discard") {
      multi_handler(originalData, connection, taskqueue);
    }
    else if(intr=='wait' && serverConfig.role==='master'){
      wait_handler(connection,command);
    } else if (intr === "ping") {
      if (serverConfig.role == "master") {
        serverConfig.master_replica_connection = connection;
        replicas_connected.add(serverConfig.master_replica_connection);
        connection.write(`+PONG\r\n`);
      } else if (serverConfig.role != "slave") {
        connection.write(`+PONG\r\n`);
      }
    } else if (intr === "echo") {
      connection.write(`$${command[1].length}\r\n${command[1]}\r\n`);
    } else if (intr === "set") {
      redisKeyValuePair.set(command[1], command[2]);
      if (command.length > 3) {
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
      const value = redisKeyValuePair.get(command[1]);
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
      const key = command[1];
      if (!redisList[key]) {
        redisList[key] = [];
      }
      for (let i = 2; i < command.length; i++) {
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
      const key = command[1];
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
      const key = command[1];
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
      x_range_handler(command[2], command[3], command, connection);
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
  }
});
server.listen(serverConfig.port, "127.0.0.1", () => {
  console.log(`server running on ${serverConfig.port}`);
});

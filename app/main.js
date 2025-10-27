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
  subsriber_commannds,
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
import { MyQueue } from "./data_structures/queue.js";
import {
  multi_handler,
  exec_hanlder,
  discard_handler,
} from "./handlers/scheduler.js";
import { createMasterConnection } from "./handlers/master_connector.js";
import {
  master_handler,
  command_propogator,
  wait_handler,
} from "./handlers/master_handler.js";
import {
  subscribe_handler,
  unsubscribe_handler,
  publish_handler,
} from "./handlers/pubsub.js";
import {
  zadd_handler,
  zrank_handler,
  zrange_handler,
  zcard_handler,
  zscore_handler,
  zrem_handler,
} from "./handlers/sortedset.js";
import { serverConfig } from "./config.js";
import { parseRDB } from "./utils/rdb_parser.js";
import path from "path";
if (serverConfig.dir && serverConfig.dbfilename) {
  const rdbPath = path.join(serverConfig.dir, serverConfig.dbfilename);
  try {
    const { keys } = parseRDB(rdbPath);
    keys.forEach((data, key) => {
      redisKeyValuePair.set(key, data);
    });
    console.log(`Loaded ${keys.size} keys from RDB file`);
  } catch (error) {
    console.log(`RDB file not found or error loading: ${error.message}`);
  }
}
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

  let subscriber_mode = { active: false };
  let subscribedChannels = new Set();

  connection.on("data", (data) => {
    const commands = parseMultipleCommands(data);
    commands.forEach((cmd) =>
      processCommand(cmd, connection, taskqueue, multi, data)
    );
  });
  function processCommand(command, connection, taskqueue, multi, originalData) {
    const intr = command[0]?.toLowerCase();
    const intru = command[0]?.toUpperCase();
    console.log(command);
    if (intr == "replconf" && command[1]?.toLowerCase() === "getack") {
      const getAckBytes = 37;
      const offsetBeforeGetAck = serverConfig.replica_offset - getAckBytes;
      const response = `*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$${
        offsetBeforeGetAck.toString().length
      }\r\n${offsetBeforeGetAck}\r\n`;
      if (serverConfig.master_replica_connection) {
        serverConfig.master_replica_connection.write(response);
      }
    } else if (
      intr == "replconf" &&
      command[1]?.toLowerCase() === "ack" &&
      serverConfig.role == "master"
    ) {
      const replicaOffset = parseInt(command[2], 10);
      if (pendingWaitRequest.active) {
        if (replicaOffset >= pendingWaitRequest.expectedOffset) {
          if (!pendingWaitRequest.ackedReplicas.has(connection)) {
            pendingWaitRequest.ackedReplicas.add(connection);

            const ackedCount = pendingWaitRequest.ackedReplicas.size;
            if (
              ackedCount >= pendingWaitRequest.numRequired ||
              ackedCount >= replicas_connected.size
            ) {
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
    }
    if (intr == "subscribe") {
      subscribe_handler(
        command,
        connection,
        subscribedChannels,
        subscriber_mode
      );
    }
    if (intr == "unsubscribe") {
      unsubscribe_handler(
        command,
        connection,
        subscribedChannels,
        subscriber_mode
      );
    }
    if (intr == "publish") {
      publish_handler(command, connection);
    }
    console.log("subscribemode", subscriber_mode.active);
    if (subscriber_mode.active) {
      if (!subsriber_commannds.includes(intru)) {
        connection.write(
          `-ERR Can't execute '${intru}' in subscribed mode\r\n`
        );
      }
      if (intru == "PING") {
        connection.write("*2\r\n$4\r\npong\r\n$0\r\n\r\n");
      }
      return;
    } else if (multi.active && intr != "exec" && intr != "discard") {
      multi_handler(originalData, connection, taskqueue);
    } else if (intr == "wait" && serverConfig.role === "master") {
      wait_handler(connection, command);
    } else if (intr == "config" && command[1] == "GET") {
      if (command[2] == "dir") {
        connection.write(
          `*2\r\n$3\r\ndir\r\n$${serverConfig.dir.length}\r\n${serverConfig.dir}\r\n`
        );
      } else if (command[2] == "dbfilename") {
        connection.write(
          `*2\r\n$10\r\ndbfilename\r\n$${serverConfig.dbfilename.length}\r\n${serverConfig.dbfilename}\r\n`
        );
      }
    } else if (intr === "ping") {
      console.log("inside ping");
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
        const actualValue =
          typeof value === "object" && value.value !== undefined
            ? value.value
            : value;

        if (typeof value === "object" && value.expireAt) {
          if (Date.now() > value.expireAt) {
            redisKeyValuePair.delete(command[1]);
            connection.write(`$-1\r\n`);
            return;
          }
        }

        connection.write(`$${actualValue.length}\r\n${actualValue}\r\n`);
      }
    } else if (intr === "keys") {
      const pattern = command[1];
      if (pattern === "*") {
        const keys = [];
        for (const [key, value] of redisKeyValuePair.entries()) {
          if (typeof value === "object" && value.expireAt) {
            if (Date.now() > value.expireAt) {
              redisKeyValuePair.delete(key);
              continue;
            }
          }
          keys.push(key);
        }
        connection.write(`*${keys.length}\r\n`);
        keys.forEach((key) => {
          connection.write(`$${key.length}\r\n${key}\r\n`);
        });
      } else {
        connection.write(`*0\r\n`);
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
    } else if (intr == "zadd") {
      zadd_handler(command, connection);
    } else if (intr == "zrank") {
      zrank_handler(command, connection);
    } else if (intr == "zrange") {
      zrange_handler(command, connection);
    } else if (intr == "zcard") {
      zcard_handler(command, connection);
    } else if (intr == "zscore") {
      zscore_handler(command, connection);
    } else if (intr == "zrem") {
      zrem_handler(command, connection);
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
    } else if (intr == "geoadd") {
      const key = command[1];
      const lon = parseDouble(command[2], 10);
      const lat = parseDouble(command[3], 10);
      const place = command[4];
      if (
        lon > 180.0 ||
        lon < -180.0 ||
        lat > 85.05112878 ||
        lat < -85.05112878
      ) {
        connection.write(`-ERR invalid latitude longitude pair\r\n`);
      } else {
        connection.write(`:1\r\n`);
      }
    }
    
  }
});
server.listen(serverConfig.port, "127.0.0.1", () => {
  console.log(`server running on ${serverConfig.port}`);
});

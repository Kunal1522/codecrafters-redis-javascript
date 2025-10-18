import net from "net";
import { expiry_checker } from "./utils/utils.js";
import { redisKeyValuePair, redisList, blpopConnections, redisStream, blocked_streams } from "./state/store.js";
import { lrange_handler, lpop_handler, blop_handler, rpush_handler } from "./handlers/lists.js";
import { xadd_handler, x_range_handler, xread_handler } from "./handlers/streams.js";

console.log("Logs from your program will appear here!");

const server = net.createServer((connection) => {
  connection.on("data", (data) => {
    const command = data.toString().split("\r\n");
    const intr = command[2]?.toLowerCase();
    console.log(command);

    if (intr === "ping") {
      connection.write(`+PONG\r\n`);
    } else if (intr === "echo") {
      connection.write(command[3] + "\r\n" + command[4] + "\r\n");
    } else if (intr === "set") {
      redisKeyValuePair.set(command[4], command[6]);
      if (command.length > 8) {
        expiry_checker(command, redisKeyValuePair);
      }
      connection.write(`+OK\r\n`);
    } else if (intr === "get") {
      const value = redisKeyValuePair.get(command[4]);
      if (value === "ille_pille_kille" || value === undefined) {
        connection.write(`$-1\r\n`);
      } else {
        connection.write(`$${value.length}\r\n${value}\r\n`);
      }
    } else if (intr === "rpush") {
      rpush_handler(command, redisList, blpopConnections, connection);
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
      connection.write(`:${redisList[key].length}\r\n`);
    } else if (intr === "llen") {
      const key = command[4];
      const len = redisList[key]?.length ?? 0;
      connection.write(`:${len}\r\n`);
    } else if (intr === "lpop") {
      lpop_handler(command, redisList, connection);
    } else if (intr === "blpop") {
      blop_handler(command, redisList, blpopConnections, connection);
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
      xadd_handler(command, connection, blocked_streams);
    } else if (intr === "xrange") {
      x_range_handler(command[6], command[8], command, connection);
    } else if (intr === "xread") {
      xread_handler(command, connection, blocked_streams);
    } else {
      connection.write("-ERR unknown command\r\n");
    }
  });
});

server.listen(6379, "127.0.0.1");

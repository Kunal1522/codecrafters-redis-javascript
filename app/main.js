import net from "net";
import { expiry_checker } from "./expiry_check.js";
import {
  lrange_handler,
  lpop_handler,
  blop_handler,
  rpush_handler,
} from "./command_handlers.js";

console.log("Logs from your program will appear here!");

// Move these OUTSIDE the connection callback so they're shared across all connections
const redis_key_value_pair = new Map();
const redis_list = {};
const blop_connections = {};
const redis_stream = {};

const server = net.createServer((connection) => {
  // Handle connection
  connection.on("data", (data) => {
    const command = data.toString().split("\r\n");
    let intr = command[2].toLowerCase();
    console.log(command);
    if (intr == "ping") connection.write(`+PONG\r\n`);
    else if (intr == "echo")
      connection.write(command[3] + "\r\n" + command[4] + "\r\n");
    else if (intr == "set") {
      redis_key_value_pair.set(command[4], command[6]);
      if (command.length > 8) {
        expiry_checker(command, redis_key_value_pair);
      }
      connection.write(`+OK\r\n`);
    } else if (intr == "get") {
      let value = redis_key_value_pair.get(command[4]);
      console.log(value);
      if (value == "ille_pille_kille") connection.write(`$-1\r\n`);
      else connection.write(`$` + value.length + `\r\n` + value + `\r\n`);
    } else if (intr == "rpush") {
      rpush_handler(command, redis_list, blop_connections, connection);
    } else if (intr == "lrange") {
      lrange_handler(command, redis_list, connection);
    } else if (intr == "lpush") {
      const key = command[4];
      if (!redis_list[key]) {
        redis_list[key] = [];
      }
      for (let i = 6; i < command.length; i += 2) {
        redis_list[key].unshift(command[i]);
      }
      connection.write(":" + redis_list[key].length + "\r\n");
    } else if (intr == "llen") {
      const key = command[4];
      if (!redis_list[key]) {
        redis_list[key] = [];
      }
      connection.write(":" + redis_list[key].length + "\r\n");
    } else if (intr == "lpop") {
      lpop_handler(command, redis_list, connection);
    } else if (intr == "blpop") {
      blop_handler(command, redis_list, blop_connections, connection);
    } else if (intr == "type") {
      const key = command[4];
      
      // Check if key exists in streams
      if (redis_stream[key]) {
        connection.write("+stream\r\n");
      }
      else if (redis_key_value_pair.has(key)) {
        connection.write("+string\r\n");
      }
      else {
        connection.write("+none\r\n");
      }
    } else if (intr == "xadd") {
      const streamKey = command[4];
      const entryId = command[6]; 
      if (!redis_stream[streamKey]) {
        redis_stream[streamKey] = [];
      }
      const entry = { id: entryId };  
      // Parse key-value pairs (starting from index 8, every 2 elements)
      for (let i = 8; i < command.length; i += 4) {
        const fieldName = command[i];
        const fieldValue = command[i + 2];
        if (fieldName && fieldValue) {
          entry[fieldName] = fieldValue;
        }
      } 
      redis_stream[streamKey].push(entry);
      connection.write(`$${entryId.length}\r\n${entryId}\r\n`);
    }
  });
});

server.listen(6379, "127.0.0.1");
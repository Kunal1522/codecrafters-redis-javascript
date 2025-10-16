import net from "net";
import { expiry_checker } from "./expiry_check.js";
import { lrange_handler } from "./handle_lrange.js";

console.log("Logs from your program will appear here!");
const server = net.createServer((connection) => {
  // Handle connection
  const redis_key_value_pair = new Map();
  const redis_list = {};
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
      const key = command[4];
      if (!redis_list[key]) {
        redis_list[key] = [];
      }
      for (let i = 6; i < command.length; i += 2) {
        redis_list[key].push(command[i]);
      }
      connection.write(":" + redis_list[key].length + "\r\n");
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
      const key = command[4];
      if (command.length > 6) {
        const element_to_pop = command[6];
        let elements_remove = [];
        for (let i = 0; i < element_to_pop; i++) {
          const top_most = redis_list[key].shift();
          if (top_most == undefined) {
            break;
          }
          elements_remove.push(top_most);
        }
        connection.write("*" + elements_remove.length + "\r\n");
        for (let i = 0; i < elements_remove.length; i++) {
          connection.write(
            "$" +
              elements_remove[i].length +
              "\r\n" +
              elements_remove[i] +
              "\r\n"
          );
        }
      } else {
        if (redis_list[key].length == 0) connection.write(`$-1\r\n`);
        else {
          const top_most = redis_list[key].shift();
          connection.write("$" + top_most.length + "\r\n" + top_most + "\r\n");
        }
      }
    }
  });
});

server.listen(6379, "127.0.0.1");

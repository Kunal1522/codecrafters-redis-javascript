import net from "net";
import { expiry_checker } from "./expiry_check.js";
import { lrange_handler, lpop_handler } from "./command_handlers.js";


console.log("Logs from your program will appear here!");
 const redis_key_value_pair = new Map();
  const redis_list = {};
  const blop_connections = {};
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
      const key = command[4];
      if (!redis_list[key]) {
        redis_list[key] = [];
      }
          for (let i = 6; i < command.length; i += 2) {
        redis_list[key].push(command[i]);
      }
      connection.write(":" + redis_list[key].length + "\r\n");
         if (!blop_connections[key]) {
        blop_connections[key] = [];
      }
        console.log("time of rpush",performance.now());
      console.log("blopconnectionlenght",blop_connections[key].length); 
      if (blop_connections[key].length > 0) {
        const top_connection=blop_connections[key].shift();
        const top_most =
          redis_list[key] && redis_list[key].length > 0
            ? redis_list[key].shift()
            : null;
            console.log("TOPMOSTELEMENT",top_most);
            console.log("top connection",top_connection);
           if (top_most == null) 
          {
            top_connection.write(`*-1\r\n`);
          }
           else
         {
          top_connection.write(`*2\r\n$${key.length}\r\n${key}\r\n$${top_most.length}\r\n${top_most}\r\n`);
         }
      }
  
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
      const key = command[4];
        console.log(key);
         if (!blop_connections[key]) {
        blop_connections[key] = [];
      }
       blop_connections[key].push(connection); 
         console.log("time of blop",performance.now());
      const timeout = command.length > 6 ? Number(command[6]) * 1000 : null;
       console.log(connection);
      if (timeout != 0) {
        setTimeout(() => {    
         const  top_connection=blop_connections[key].shift(); 
          const top_most =
            redis_list[key] && redis_list[key].length > 0
              ? redis_list[key].shift():null;
         if (top_most == null) top_connection.write(`*-1\r\n`);
        else
         {
          top_connection.write(`*2\r\n$${key.length}\r\n${key}\r\n$${top_most.length}\r\n${top_most}\r\n`);
         }        
        }, timeout);
      }
    }
  });
});

server.listen(6379, "127.0.0.1");

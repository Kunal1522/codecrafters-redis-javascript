const net = require("net");

// You can use print statements as follows for debugging, they'll be visible when running tests.
const s = "kunal";
let k = s.length;

console.log(k);
console.log("Logs from your program will appear here!");
const server = net.createServer((connection) => {
  // Handle connection

  const redis_key_value_pair = new Map();
  connection.on("data", (data) => {
    const command = data.toString().split("\r\n");
    console.log(command);
    if (command[2].toLowerCase() == "ping") connection.write(`+PONG\r\n`);
    else if (command[2].toLowerCase() == "echo")
      connection.write(command[3] + "\r\n" + command[4] + "\r\n");
    else if (command[2].toLowerCase() == "set") {
      redis_key_value_pair.set(command[4], command[6]);
      if (command.length > 8) {
        if (command[8].toLowerCase() == "px") {
          const expiry = command[10];
          console.log(expiry);
          setTimeout(() => {
            redis_key_value_pair.set(command[4], `ille_pille_kille`);
          }, expiry);
        } else if (command[8].toLowerCase() == "ex") {
          const expiry = command[10];
          console.log(expiry);
          setTimeout(() => {
            redis_key_value_pair.set(command[4], `ille_pille_kille`);
          }, expiry * 1000);
        }
      }
      connection.write(`+OK\r\n`);
    } else if (command[2].toLowerCase() == "get") {
      let value = redis_key_value_pair.get(command[4]);
      console.log(value);
      if (value == "ille_pille_kille") connection.write(`$-1\r\n`);
      else connection.write(`$` + value.length + `\r\n` + value + `\r\n`);
    }
  });
});

server.listen(6379, "127.0.0.1");

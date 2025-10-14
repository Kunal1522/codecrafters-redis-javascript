const net = require("net");

// You can use print statements as follows for debugging, they'll be visible when running tests.
console.log("Logs from your program will appear here!");

const server = net.createServer((connection) => {
  // Handle connection
  connection.on("data",(data) => {
     const command=data.toString().split('\r\n');
     if(command[2].toLowerCase()=="ping")
        connection.write(`+PONG\r\n`);
    else if(command[2].toLowerCase()=='echo')
        connection.write(command[3]+'\r\n'+command[4]+'\r\n');
  });
});

server.listen(6379, "127.0.0.1");

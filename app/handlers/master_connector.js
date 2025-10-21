import net from "net";
import { serverConfig } from "../config.js";
console.log("server configuration", serverConfig);
function createMasterConnection() {
  const connection = net.createConnection(
    { port: serverConfig.master_port, host: "127.0.0.1" },
    () => {
      console.log("connected to master");
      connection.write(`*1\r\n$4\r\nPING\r\n`);

      connection.write(
        "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n"
      );
      connection.write(
        "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
      );
    }
  );
  connection.on("data", (data) => {
    console.log(data);
  });
  return connection;
}

export { createMasterConnection };

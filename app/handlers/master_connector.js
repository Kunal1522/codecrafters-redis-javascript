import net from "net";
import { serverConfig } from "../config.js";
console.log("server configuration", serverConfig);
function createMasterConnection() {
  const connection = net.createConnection(
    { port: serverConfig.master_port, host: "127.0.0.1" },
    () => {
      console.log("connected to master");
      connection.write(`*1\r\n$4\r\nPING\r\n`);
      setTimeout(() => {
        connection.write(
          "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n"
        );
      }, 100);
      setTimeout(() => {
        connection.write(
          "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
        );
      }, 100);
      setTimeout(() => {
        connection.write(`*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n`);
      }, 1000);
    }
  );
  connection.on("data", (data) => {
    console.log(data);
  });
  return connection;
}

export { createMasterConnection };

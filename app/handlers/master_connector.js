import net from "net";
import { serverConfig } from "../config.js";
console.log("server configuration",serverConfig);
function createMasterConnection() {
  const connection = net.createConnection(
    { port: serverConfig.master_port, host: "127.0.0.1"},
    () => {
      console.log("connected to master");
      connection.write(`*1\r\n$4\r\nPING\r\n`);
    }
  );
  return connection;
}

export {createMasterConnection};

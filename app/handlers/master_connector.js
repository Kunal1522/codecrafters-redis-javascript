import net from "net";
import { serverConfig } from "../config.js";

console.log("server configuration", serverConfig);

function setupReplicaProxy() {
  const replicaBridgeConnection = net.createConnection(
    { port: serverConfig.port, host: "127.0.0.1" },
    () => console.log("Replica bridge connected to local server")
  );
  
  replicaBridgeConnection.on("error", (err) => {
    console.error("Replica bridge connection error:", err.message);
  });
  
  return replicaBridgeConnection; 
}

function createMasterConnection() {
  let handshakeComplete = false;
  let rdbBytesReceived = 0;
  
  const connection = net.createConnection(
    { port: serverConfig.master_port, host: "127.0.0.1" },
    () => {
      console.log("Connected to master");
      connection.write(`*1\r\n$4\r\nPING\r\n`);

      setTimeout(() => {
        connection.write("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n");
      }, 100);

      setTimeout(() => {
        connection.write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n");
      }, 100);

      setTimeout(() => {
        connection.write(`*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n`);
      }, 1000);
    }
  );

  connection.on("error", (err) => {
    console.error("Master connection error:", err.message);
  });

  let replicaBridgeConnection = null;
  setTimeout(() => {
    replicaBridgeConnection = setupReplicaProxy();
    
    connection.on("data", (data) => {
      if (!handshakeComplete) {
        const dataStr = data.toString();
        if (dataStr.includes('REDIS') || data[0] === 0x52) {
          handshakeComplete = true;
          rdbBytesReceived = connection.bytesRead;
          serverConfig.replica_offset = 0;
        }
      } else {
        const offsetBeforeThisCommand = connection.bytesRead - rdbBytesReceived - data.length;
        serverConfig.replica_offset = offsetBeforeThisCommand;
        if (replicaBridgeConnection) {
          replicaBridgeConnection.write(data);
        }
      }
    });
  }, 1000);

  console.log("Allocated master-replica connection");
  serverConfig.master_replica_connection = connection;
}

export { createMasterConnection };

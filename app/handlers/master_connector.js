import net from "net";
import { serverConfig } from "../config.js";

console.log("server configuration", serverConfig);

function setupReplicaProxy() {
  // Create a local bridge (to forward data from master → client)
  // This connection forwards commands from master to the replica's own server
  const replicaBridgeConnection = net.createConnection(
    { port: serverConfig.port, host: "127.0.0.1" },
    () => {
      console.log("Replica bridge connected to local server");
    }
  );
  
  replicaBridgeConnection.on("error", (err) => {
    console.error("Replica bridge connection error:", err.message);
  });
  
  return replicaBridgeConnection; 
}

// Here this instance becomes the replica
function createMasterConnection() {
  const connection = net.createConnection(
    { port: serverConfig.master_port, host: "127.0.0.1" },
    () => {
      console.log("Connected to master");
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

  connection.on("error", (err) => {
    console.error("Master connection error:", err.message);
  });

  // Setup bridge connection after a delay to ensure local server is listening
  let replicaBridgeConnection = null;
  setTimeout(() => {
    replicaBridgeConnection = setupReplicaProxy();
    
    connection.on("data", (data) => {
      if (replicaBridgeConnection) {
        replicaBridgeConnection.write(data);
      }
    });
  }, 500);

  console.log("Allocated master-replica connection");
  serverConfig.master_replica_connection = connection;
}

export { createMasterConnection };

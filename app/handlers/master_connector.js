import net from "net";
import { serverConfig } from "../config.js";

console.log("server configuration", serverConfig);

function setupReplicaProxy() {
  // Create a local bridge (to forward data from master → client)
  //this connection i am creating betwene the two sockets running on replica // one which does hanshake -----other which acts as //the reason being master will propagate to handshake one and this handshake will forward it to //client .so i don't need to refactor the client architeture ......i am lazy lazy
  const replicaBridgeConnection = net.createConnection(
    { port: serverConfig.port, host: "127.0.0.1" },
    () => {
      console.log("Replica bridge connected to local server");
    }
  );

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


  const replicaBridgeConnection = setupReplicaProxy();

  connection.on("data", (data) => {
    replicaBridgeConnection.write(data);
  });

  console.log("Allocated master-replica connection");
  serverConfig.master_replica_connection = connection;
}

export { createMasterConnection };

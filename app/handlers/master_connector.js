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
      console.log("Received from master, bytesRead:", connection.bytesRead, "dataLen:", data.length);
      if (!handshakeComplete) {
        const dataStr = data.toString('latin1');
        const hasFullresync = dataStr.includes('FULLRESYNC');
        const hasRdbPrefix = dataStr.includes('$88') || dataStr.includes('$');
        const hasRedis = dataStr.includes('REDIS') || data.includes(Buffer.from('REDIS'));
        
        if (hasFullresync || hasRedis || hasRdbPrefix) {
          console.log("Handshake/RDB detected, setting handshakeComplete");
          handshakeComplete = true;
          
          // Check if there are commands after RDB in the same buffer
          const rdbEndMatch = dataStr.match(/\$(\d+)\r\n/);
          if (rdbEndMatch) {
            const rdbSize = parseInt(rdbEndMatch[1]);
            const rdbStartIndex = dataStr.indexOf(rdbEndMatch[0]) + rdbEndMatch[0].length;
            const rdbEndIndex = rdbStartIndex + rdbSize;
            
            // Set rdbBytesReceived to point at end of RDB (not end of buffer)
            rdbBytesReceived = connection.bytesRead - data.length + rdbEndIndex;
            
            if (rdbEndIndex < data.length) {
              // There's data after RDB, forward it to bridge
              const commandsAfterRdb = data.slice(rdbEndIndex);
              console.log("Commands after RDB detected, forwarding", commandsAfterRdb.length, "bytes");
              if (replicaBridgeConnection) {
                replicaBridgeConnection.write(commandsAfterRdb);
              }
              serverConfig.replica_offset = commandsAfterRdb.length;
            } else {
              serverConfig.replica_offset = 0;
            }
          } else {
            rdbBytesReceived = connection.bytesRead;
            serverConfig.replica_offset = 0;
          }
        }
      } else {
        console.log("Forwarding to bridge, offset:", connection.bytesRead - rdbBytesReceived);
        if (replicaBridgeConnection) {
          replicaBridgeConnection.write(data);
        }
        serverConfig.replica_offset = connection.bytesRead - rdbBytesReceived;
      }
    });
  }, 1000);

  console.log("Allocated master-replica connection");
  serverConfig.master_replica_connection = connection;
}

export { createMasterConnection };

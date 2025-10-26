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
  let rdbSize = null;
  let rdbStartPosition = null;

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
  let replicaBridgeConnection = null;
  setTimeout(() => {
    replicaBridgeConnection = setupReplicaProxy();
    connection.on("data", (data) => {
      console.log(
        "Received from master, bytesRead:",
        connection.bytesRead,
        "dataLen:",
        data.length
      );

      if (!handshakeComplete) {
        const dataStr = data.toString("latin1");

        // Detect RDB size from $<size>\r\n
        if (rdbSize === null) {
          const rdbMatch = dataStr.match(/\$(\d+)\r\n/);
          if (rdbMatch) {
            rdbSize = parseInt(rdbMatch[1]);
            const rdbPrefixEnd =
              dataStr.indexOf(rdbMatch[0]) + rdbMatch[0].length;
            rdbStartPosition =
              connection.bytesRead - data.length + rdbPrefixEnd;
            console.log(
              "RDB detected, size:",
              rdbSize,
              "startPos:",
              rdbStartPosition
            );
          }
        }

        // Check if RDB is complete
        if (rdbSize !== null) {
          const rdbEndPosition = rdbStartPosition + rdbSize;
          if (connection.bytesRead >= rdbEndPosition) {
            console.log("RDB complete, setting handshakeComplete");
            handshakeComplete = true;
            rdbBytesReceived = rdbEndPosition;
            serverConfig.replica_offset = 0;

            // Check if there are commands after RDB in current buffer
            const currentBufferStart = connection.bytesRead - data.length;
            if (rdbEndPosition > currentBufferStart) {
              const offsetInBuffer = rdbEndPosition - currentBufferStart;
              if (offsetInBuffer < data.length) {
                const commandsAfterRdb = data.slice(offsetInBuffer);
                console.log(
                  "Commands after RDB in buffer, forwarding",
                  commandsAfterRdb.length,
                  "bytes"
                );
                if (replicaBridgeConnection) {
                  replicaBridgeConnection.write(commandsAfterRdb);
                }
                serverConfig.replica_offset = commandsAfterRdb.length;
              }
            }
          }
        }
      } else {
        console.log(
          "Forwarding to bridge, offset:",
          connection.bytesRead - rdbBytesReceived
        );
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

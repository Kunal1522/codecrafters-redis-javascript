import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { serverConfig } from "../config.js";
import {
  replicas_connected,
  REPLICATABLE_COMMANDS,
  master_offset,
} from "../state/store.js";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
function master_handler(command, connection) {
  const intr = command[0]?.toLowerCase();
  if (intr == "psync") {
    connection.write(`+FULLRESYNC ${serverConfig.master_replid} 0\r\n`);
    const rdbPath = path.join(__dirname, "empty.rdb");
    const binary_data = fs.readFileSync(rdbPath);
    console.log("rdb data read", binary_data);
    const bin_length = binary_data.length;
    connection.write(`$${bin_length}\r\n`);
    connection.write(binary_data);
    replicas_connected.add(connection);
    return;
  }
}
function command_propogator(command, data) {
  console.log(command);
  const intr = command[0]?.toUpperCase();
  console.log(intr);
  if (REPLICATABLE_COMMANDS.includes(intr) && serverConfig.role === "master") {
    console.log(replicas_connected);
    for (const replica_connection of replicas_connected) {
      replica_connection.write(data);
      master_offset.set(replica_connection, replica_connection.bytesWritten);
    }
  }
}

function wait_handler(connection, command) {
  const numReplicasRequired = parseInt(command[1], 10);
  const timeout = parseInt(command[2], 10);
  
  if (replicas_connected.size === 0) {
    connection.write(`:0\r\n`);
    return;
  }
  
  let ackedCount = 0;
  const replicaAcks = new Map();
  let timeoutId = null;
  
  const sendResponse = () => {
    if (timeoutId) clearTimeout(timeoutId);
    connection.write(`:${ackedCount}\r\n`);
  };
  
  const checkIfComplete = () => {
    if (ackedCount >= numReplicasRequired || ackedCount >= replicas_connected.size) {
      sendResponse();
      return true;
    }
    return false;
  };
  
  let allReplicasUpToDate = true;
  for (const replica of replicas_connected) {
    const expectedOffset = master_offset.get(replica) || 0;
    if (expectedOffset > 0) {
      allReplicasUpToDate = false;
      break;
    }
  }
  
  if (allReplicasUpToDate) {
    connection.write(`:${replicas_connected.size}\r\n`);
    return;
  }
  
  timeoutId = setTimeout(() => {
    sendResponse();
  }, timeout);
  
  for (const replica of replicas_connected) {
    const expectedOffset = master_offset.get(replica) || 0;
    
    replica.write(`*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n`);
    
    const onData = (data) => {
      const dataStr = data.toString();
      if (dataStr.includes('REPLCONF') && dataStr.includes('ACK')) {
        const match = dataStr.match(/ACK\r\n\$(\d+)\r\n(\d+)/);
        if (match) {
          const replicaOffset = parseInt(match[2], 10);
          
          if (!replicaAcks.has(replica) && replicaOffset >= expectedOffset) {
            replicaAcks.set(replica, true);
            ackedCount++;
            
            if (checkIfComplete()) {
              replica.removeListener('data', onData);
            }
          }
        }
      }
    };
    
    replica.on('data', onData);
    
    setTimeout(() => {
      replica.removeListener('data', onData);
    }, timeout + 100);
  }
}
export { master_handler, command_propogator, wait_handler };

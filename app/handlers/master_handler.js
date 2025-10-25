import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { serverConfig } from "../config.js";
import {
  replicas_connected,
  REPLICATABLE_COMMANDS,
  master_offset,
  pendingWaitRequest,
} from "../state/store.js";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
function master_handler(command, connection) {
  const intr = command[0]?.toLowerCase();
  if (intr == "psync") {
    connection.write(`+FULLRESYNC ${serverConfig.master_replid} 0\r\n`);
    const rdbPath = path.join(__dirname, "empty.rdb");
    const binary_data = fs.readFileSync(rdbPath);
    const bin_length = binary_data.length;
    connection.write(`$${bin_length}\r\n`);
    connection.write(binary_data);
    replicas_connected.add(connection);
    master_offset.set(connection, 0);
    return;
  }
}
function command_propogator(command, data) {
  const intr = command[0]?.toUpperCase();
  if (REPLICATABLE_COMMANDS.includes(intr) && serverConfig.role === "master") {
    for (const replica_connection of replicas_connected) {
      replica_connection.write(data);
      const currentOffset = master_offset.get(replica_connection) || 0;
      master_offset.set(replica_connection, currentOffset + data.length);
    }
  }
}

function wait_handler(connection, command) {
  const numReplicasRequired = parseInt(command[1], 10);
  const timeout = parseInt(command[2], 10);
  
  if (pendingWaitRequest.timeoutId) {
    clearTimeout(pendingWaitRequest.timeoutId);
  }
  /*
  const pendingWaitRequest = {
  active: false,
  clientConnection: null,
  numRequired: 0,
  timeout: 0,
  ackedReplicas: new Set(),
  timeoutId: null,
  replicaExpectedOffsets: new Map()
};*/ 
  pendingWaitRequest.active = false;
  pendingWaitRequest.ackedReplicas.clear();
  
  if (replicas_connected.size === 0) {
    connection.write(`:0\r\n`);
    return;
  }
  
  const currentMasterOffset = Math.max(...Array.from(master_offset.values()), 0);
  if (currentMasterOffset === 0) {
    connection.write(`:${replicas_connected.size}\r\n`);
    return;
  }
  pendingWaitRequest.active = true;
  pendingWaitRequest.clientConnection = connection;
  pendingWaitRequest.numRequired = numReplicasRequired;
  pendingWaitRequest.timeout = timeout;
  pendingWaitRequest.expectedOffset = currentMasterOffset;
  
  for (const replica of replicas_connected) {
    replica.write(`*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n`);
  }
  
  const checkAndRespond = () => {
    if (!pendingWaitRequest.active) return;
    
    const ackedCount = pendingWaitRequest.ackedReplicas.size;
    if (ackedCount >= numReplicasRequired || ackedCount >= replicas_connected.size) {
      if (pendingWaitRequest.timeoutId) {
        clearTimeout(pendingWaitRequest.timeoutId);
      }
      connection.write(`:${ackedCount}\r\n`);
      pendingWaitRequest.active = false;
      pendingWaitRequest.ackedReplicas.clear();
      return true;
    }
    return false;
  };
  
  pendingWaitRequest.timeoutId = setTimeout(() => {
    if (!pendingWaitRequest.active) return;
    
    const ackedCount = pendingWaitRequest.ackedReplicas.size;
    connection.write(`:${ackedCount}\r\n`);
    pendingWaitRequest.active = false;
    pendingWaitRequest.ackedReplicas.clear();
  }, timeout);
  
  setTimeout(() => checkAndRespond(), 10);
}
export { master_handler, command_propogator, wait_handler };


import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { serverConfig } from "../config.js";
import { replicas_connected,REPLICATABLE_COMMANDS } from "../state/store.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
function master_handler(command, connection) {
  const intr = command[2]?.toLowerCase();
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
function command_propogator(command,data)
{
    console.log(command);
    const intr=command[2]?.toUpperCase();
    console.log(intr);
    if(REPLICATABLE_COMMANDS.includes(intr) && serverConfig.role==="master")
    {
           console.log(replicas_connected);
         for(const replica_connections of replicas_connected)
         {
                replica_connections.write(data); 
         }
    }
}
export { master_handler,command_propogator };

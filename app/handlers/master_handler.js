import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { serverConfig } from "../config.js";

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
    // Write length header as string, then binary data separately
    connection.write(`$${bin_length}\r\n`);
    connection.write(binary_data);
    return;
  }
}
export { master_handler };

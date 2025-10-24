import net from "net";
import {
  redisKeyValuePair,
  redisList,
  blpopConnections,
  redisStream,
  blocked_streams,
} from "../state/store.js";
import {
  lrange_handler,
  lpop_handler,
  blop_handler,
  rpush_handler,
  incr_handler,
} from "./lists.js";
import { xadd_handler, x_range_handler, xread_handler } from "./streams.js";
import { expiry_checker } from "../utils/utils.js";
import { serverConfig } from "../config.js";

function multi_handler(buffer_data, connection, taskqueue) {
  let task = {
    connection: connection,
    buffer_data: buffer_data,
  };
  taskqueue.push(task);
  connection.write("+QUEUED\r\n");
}
function exec_hanlder(buffer_data, connection, taskqueue, multi) {
  if (!multi.active) {
    connection.write(`-ERR EXEC without MULTI\r\n`);
    return;
  }
  if (taskqueue.empty()) {
    connection.write("*0\r\n");
    multi.active = false;
    return;
  }

  const results = [];
  let commandsToExecute = [];
  
  // Collect all commands from queue
  while (!taskqueue.empty()) {
    const task = taskqueue.pop();
    commandsToExecute.push(task.buffer_data);
  }
  const clientConnection = net.createConnection({ port: serverConfig.port, host: "127.0.0.1" }, () => {
    console.log(`Exec handler connected to Redis server on port ${serverConfig.port}`);
    sendNextCommand();
  });

  let commandIndex = 0;
  let responseBuffer = "";

  function sendNextCommand() {
    if (commandIndex < commandsToExecute.length) {
      const buffer_data = commandsToExecute[commandIndex];
      console.log(`Sending command ${commandIndex + 1}/${commandsToExecute.length}`);
      
      // Send the command
      if (typeof buffer_data === "string") {
        clientConnection.write(Buffer.from(buffer_data, "utf-8"));
      } else {
        clientConnection.write(buffer_data);
      }
    } else {
      // All commands sent, close connection when all responses received
      clientConnection.end();
    }
  }

  clientConnection.on("data", (data) => {
    responseBuffer += data.toString();
    
    // Try to parse one complete response
    const response = parseOneResponse(responseBuffer);
    
    if (response !== null) {
      // We got a complete response
      results.push(response.data);
      responseBuffer = response.remaining;
      commandIndex++;
      
      // Send the next command
      sendNextCommand();
    }
  });

  clientConnection.on("end", () => {
    // Send all collected results back to the original connection
    connection.write(`*${results.length}\r\n`);
    results.forEach((result) => {
      connection.write(result);
    });
    
    multi.active = false;
    console.log("is_multi", multi.active);
  });

  clientConnection.on("error", (error) => {
    console.error("Error connecting to Redis server:", error);
    connection.write(`-ERR ${error.message}\r\n`);
    multi.active = false;
  });
}

function parseResponses(buffer) {
  const responses = [];
  let remaining = buffer;
  let count = 0;

  while (remaining.length > 0) {
    const response = parseOneResponse(remaining);
    
    if (response === null) {
      break;
    }
    
    responses.push(response.data);
    remaining = response.remaining;
    count++;
  }

  return { data: responses, count: count, remaining: remaining };
}


function parseOneResponse(buffer) {
  if (buffer.length === 0) return null;
  const firstChar = buffer[0];
  const crlfIndex = buffer.indexOf("\r\n");

  if (crlfIndex === -1) return null;

  if (firstChar === "+") {

    const response = buffer.substring(0, crlfIndex + 2);
    return { data: response, remaining: buffer.substring(crlfIndex + 2) };
  } else if (firstChar === "-") {

    const response = buffer.substring(0, crlfIndex + 2);
    return { data: response, remaining: buffer.substring(crlfIndex + 2) };
  } else if (firstChar === ":") {

    const response = buffer.substring(0, crlfIndex + 2);
    return { data: response, remaining: buffer.substring(crlfIndex + 2) };
  } else if (firstChar === "$") {

    const lengthStr = buffer.substring(1, crlfIndex);
    const length = parseInt(lengthStr);

    if (length === -1) {

      const response = buffer.substring(0, crlfIndex + 2);
      return { data: response, remaining: buffer.substring(crlfIndex + 2) };
    }

    const totalLength = crlfIndex + 2 + length + 2;
    if (buffer.length < totalLength) return null;

    const response = buffer.substring(0, totalLength);
    return { data: response, remaining: buffer.substring(totalLength) };
  } else if (firstChar === "*") {
    const lengthStr = buffer.substring(1, crlfIndex);
    const arrayLength = parseInt(lengthStr);

    if (arrayLength === 0) {
   
      const response = buffer.substring(0, crlfIndex + 2);
      return { data: response, remaining: buffer.substring(crlfIndex + 2) };
    }

    if (arrayLength === -1) {
 
      const response = buffer.substring(0, crlfIndex + 2);
      return { data: response, remaining: buffer.substring(crlfIndex + 2) };
    }

 
    let pos = crlfIndex + 2;
    let elementsParsed = 0;

    for (let i = 0; i < arrayLength; i++) {
      const remainingBuffer = buffer.substring(pos);
      const elementResponse = parseOneResponse(remainingBuffer);

      if (elementResponse === null) {
        // Incomplete array
        return null;
      }

      pos += remainingBuffer.length - elementResponse.remaining.length;
      elementsParsed++;
    }

    const response = buffer.substring(0, pos);
    return { data: response, remaining: buffer.substring(pos) };
  }

  return null; 
}

function discard_handler(buffer_data, connection, taskqueue, multi) {
   if (!multi.active) {
    connection.write(`-ERR DISCARD without MULTI\r\n`);
    return;
  }
  while (!taskqueue.empty()) {
    taskqueue.pop();
  }
  connection.write("+OK\r\n");
  multi.active=false;
}
export { multi_handler, exec_hanlder,discard_handler };

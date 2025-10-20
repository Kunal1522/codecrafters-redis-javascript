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

function multi_handler(command, connection, taskqueue) {
  let task = {
    connection: connection,
    command: command,
  };
  taskqueue.push(task);
  connection.write("+QUEUED\r\n");
}
function executeCommand(command, connection) {
  const intr = command[2]?.toLowerCase();

  try {
    if (intr === "set") {
      redisKeyValuePair.set(command[4], command[6]);
      if (command.length > 8) {
        expiry_checker(command, redisKeyValuePair);
      }
      return `+OK\r\n`;
    } else if (intr === "get") {
      const value = redisKeyValuePair.get(command[4]);
      if (value === "ille_pille_kille" || value === undefined) {
        return `$-1\r\n`;
      } else {
        return `$${value.length}\r\n${value}\r\n`;
      }
    } else if (intr === "incr") {
      const key = command[4];
      const current = parseInt(redisKeyValuePair.get(key) || 0);
      const newValue = current + 1;
      redisKeyValuePair.set(key, newValue.toString());
      return `:${newValue}\r\n`;
    } else if (intr === "lpush") {
      const key = command[4];
      if (!redisList[key]) {
        redisList[key] = [];
      }
      for (let i = 6; i < command.length; i += 2) {
        if (command[i]) redisList[key].unshift(command[i]);
      }
      return `:${redisList[key].length}\r\n`;
    } else if (intr === "rpush") {
      rpush_handler(command, redisList, blpopConnections, connection);
      return ``;
    } else if (intr === "lrange") {
      const key = command[4];
      const start = parseInt(command[6]);
      const stop = parseInt(command[8]);
      const list = redisList[key] || [];
      const range = list.slice(start, stop + 1);
      let response = `*${range.length}\r\n`;
      range.forEach((item) => {
        response += `$${item.length}\r\n${item}\r\n`;
      });
      return response;
    } else if (intr === "llen") {
      const key = command[4];
      const len = redisList[key]?.length ?? 0;
      return `:${len}\r\n`;
    } else if (intr === "lpop") {
      const key = command[4];
      const list = redisList[key];
      if (!list || list.length === 0) {
        return `$-1\r\n`;
      }
      const value = list.shift();
      return `$${value.length}\r\n${value}\r\n`;
    } else if (intr === "type") {
      const key = command[4];
      if (redisStream[key]) {
        return `+stream\r\n`;
      } else if (redisList[key]) {
        return `+list\r\n`;
      } else if (redisKeyValuePair.has(key)) {
        return `+string\r\n`;
      } else {
        return `+none\r\n`;
      }
    } else {
      return `-ERR unknown command\r\n`;
    }
  } catch (error) {
    console.error("Error executing command:", error);
    return `-ERR ${error.message}\r\n`;
  }
}

function exec_hanlder(command, connection, taskqueue, multi) {
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
  while (!taskqueue.empty()) {
    const task = taskqueue.pop();
    const { connection: taskConnection, command: taskCommand } = task;

    const result = executeCommand(taskCommand, connection);
    results.push(result);
  }

  connection.write(`*${results.length}\r\n`);
  results.forEach((result) => {
    if (result) connection.write(result);
  });

  multi.active = false;
  console.log("is_multi", multi.active);
}

function discard_handler(command, connection, taskqueue, multi) {
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

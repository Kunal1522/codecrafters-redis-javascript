import { writeToConnection } from "../utils/utils.js";
import { REPLICATABLE_COMMANDS } from "../state/store.js";

function lrange_handler(command, redis_list, connection) {
  const key = command[1];
  let start = Number(command[2]);
  let stop = Number(command[3]);
  if (start < 0) start = redis_list[key].length + start;
  if (stop < 0) stop = redis_list[key].length + stop;
  start = Math.max(start, 0);
  stop = Math.max(stop, 0);
  start = Math.min(start, redis_list[key] ? redis_list[key].length - 1 : 0);
  stop = Math.min(stop, redis_list[key] ? redis_list[key].length - 1 : 0);
  if (!redis_list[key]) {
    connection.write("*0\r\n");
    return;
  }
  const list = redis_list[key];
  const end = Math.min(stop, list.length - 1);
  if (
    start >= list.length ||
    start > stop ||
    stop < 0 ||
    list.length == 0 ||
    start < 0
  ) {
    connection.write("*0\r\n");
    return;
  }
  connection.write(`*${end - start + 1}\r\n`);
  for (let i = start; i <= end; i++) {
    const val = list[i];
    connection.write(`$${val.length}\r\n${val}\r\n`);
  }
}

function lpop_handler(command, redis_list, connection, serverConfig) {
  const key = command[1];
  if (command.length > 2) {
    const element_to_pop = command[2];
    let elements_remove = [];
    for (let i = 0; i < element_to_pop; i++) {
      const top_most = redis_list[key].shift();
      if (top_most == undefined) {
        break;
      }
      elements_remove.push(top_most);
    }
    writeToConnection(connection, "*" + elements_remove.length + "\r\n", "lpop", serverConfig, REPLICATABLE_COMMANDS);
    for (let i = 0; i < elements_remove.length; i++) {
      writeToConnection(connection,
        "$" + elements_remove[i].length + "\r\n" + elements_remove[i] + "\r\n",
        "lpop", serverConfig, REPLICATABLE_COMMANDS
      );
    }
  } else {
    if (redis_list[key].length == 0) {
      writeToConnection(connection, `$-1\r\n`, "lpop", serverConfig, REPLICATABLE_COMMANDS);
    } else {
      const top_most = redis_list[key].shift();
      writeToConnection(connection, "$" + top_most.length + "\r\n" + top_most + "\r\n", "lpop", serverConfig, REPLICATABLE_COMMANDS);
    }
  }
}

function blop_handler(command, redis_list, blop_connections, connection, serverConfig) {
  const key = command[1];
  if (!blop_connections[key]) {
    blop_connections[key] = [];
  }
  blop_connections[key].push(connection);
  const timeout = command.length > 2 ? Number(command[2]) * 1000 : null;
  if (timeout != 0) {
    setTimeout(() => {
      const top_connection = blop_connections[key].shift();
      const top_most =
        redis_list[key] && redis_list[key].length > 0
          ? redis_list[key].shift()
          : null;
      if (top_most == null) {
        writeToConnection(top_connection, `*-1\r\n`, "blpop", serverConfig, REPLICATABLE_COMMANDS);
      } else {
        writeToConnection(top_connection,
          `*2\r\n$${key.length}\r\n${key}\r\n$${top_most.length}\r\n${top_most}\r\n`,
          "blpop", serverConfig, REPLICATABLE_COMMANDS
        );
      }
    }, timeout);
  }
}

function rpush_handler(command, redis_list, blop_connections, connection, serverConfig) {
  const key = command[1];
  if (!redis_list[key]) {
    redis_list[key] = [];
  }
  for (let i = 2; i < command.length; i++) {
    redis_list[key].push(command[i]);
  }
  writeToConnection(connection, ":" + redis_list[key].length + "\r\n", "rpush", serverConfig, REPLICATABLE_COMMANDS);
  if (!blop_connections[key]) {
    blop_connections[key] = [];
  }
  if (blop_connections[key].length > 0) {
    const top_connection = blop_connections[key].shift();
    const top_most =
      redis_list[key] && redis_list[key].length > 0
        ? redis_list[key].shift()
        : null;
    if (top_most == null) {
      top_connection.write(`*-1\r\n`);
    } else {
      top_connection.write(
        `*2\r\n$${key.length}\r\n${key}\r\n$${top_most.length}\r\n${top_most}\r\n`
      );
    }
  }
}
function incr_handler(command, redis_key_value, connection, serverConfig) {
  const key = command[1];
  console.log(typeof redis_key_value.get(key));
  if (redis_key_value.get(key) == undefined) {
    redis_key_value.set(key, 0);
  }
  if (isNaN(Number(redis_key_value.get(key)))) {
    connection.write("-ERR value is not an integer or out of range\r\n");
    return;
  }
  const value = Number(redis_key_value.get(key));
  let newValue = Number(value) + 1;
  redis_key_value.set(key, String(newValue));
  writeToConnection(connection, `:${newValue}\r\n`, "incr", serverConfig, REPLICATABLE_COMMANDS);
}


export {
  lrange_handler,
  lpop_handler,
  blop_handler,
  rpush_handler,
  incr_handler,
};

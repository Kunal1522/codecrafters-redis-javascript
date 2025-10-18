import net from "net";
import { expiry_checker } from "./utils/utils.js";

import {
  lrange_handler,
  lpop_handler,
  blop_handler,
  rpush_handler,
} from "./command_handlers.js";
const streamSequenceMap = new Map();

console.log("Logs from your program will appear here!");

const redis_key_value_pair = new Map();
const redis_list = {};
const blop_connections = {};
const redis_stream = {};
function generateStreamId(rawId) {
  if (!rawId) return null;
  if (rawId.includes("-") && !rawId.endsWith("-*")) {
    return rawId;
  }
  let timestamp, sequence;
  if (rawId.endsWith("-*")) {
    timestamp = rawId.split("-")[0];
  } else if (rawId === "*") {
    timestamp = Date.now();
  }
  const prevSeq = streamSequenceMap.get(timestamp) ?? -1;

  sequence = prevSeq + 1;
  if (timestamp == 0 && sequence == 0) sequence++;
  streamSequenceMap.set(timestamp, sequence);

  const fullId = `${timestamp}-${sequence}`;
  return fullId;
}

const server = net.createServer((connection) => {
  // Handle connection
  connection.on("data", (data) => {
    const command = data.toString().split("\r\n");
    let intr = command[2].toLowerCase();
    console.log(command);
    if (intr == "ping") connection.write(`+PONG\r\n`);
    else if (intr == "echo")
      connection.write(command[3] + "\r\n" + command[4] + "\r\n");
    else if (intr == "set") {
      redis_key_value_pair.set(command[4], command[6]);
      if (command.length > 8) {
        expiry_checker(command, redis_key_value_pair);
      }
      connection.write(`+OK\r\n`);
    } else if (intr == "get") {
      let value = redis_key_value_pair.get(command[4]);
      console.log(value);
      if (value == "ille_pille_kille") connection.write(`$-1\r\n`);
      else connection.write(`$` + value.length + `\r\n` + value + `\r\n`);
    } else if (intr == "rpush") {
      rpush_handler(command, redis_list, blop_connections, connection);
    } else if (intr == "lrange") {
      lrange_handler(command, redis_list, connection);
    } else if (intr == "lpush") {
      const key = command[4];
      if (!redis_list[key]) {
        redis_list[key] = [];
      }
      for (let i = 6; i < command.length; i += 2) {
        redis_list[key].unshift(command[i]);
      }
      connection.write(":" + redis_list[key].length + "\r\n");
    } else if (intr == "llen") {
      const key = command[4];
      if (!redis_list[key]) {
        redis_list[key] = [];
      }
      connection.write(":" + redis_list[key].length + "\r\n");
    } else if (intr == "lpop") {
      lpop_handler(command, redis_list, connection);
    } else if (intr == "blpop") {
      blop_handler(command, redis_list, blop_connections, connection);
    } else if (intr == "type") {
      const key = command[4];
      if (redis_stream[key]) {
        connection.write("+stream\r\n");
      } else if (redis_key_value_pair.has(key)) {
        connection.write("+string\r\n");
      } else {
        connection.write("+none\r\n");
      }
    } else if (intr == "xadd") {
      const streamKey = command[4];
      let entryId = command[6];
      entryId = generateStreamId(entryId);
      if (!redis_stream[streamKey]) {
        redis_stream[streamKey] = [];
      }
      console.log("entryId:", entryId);

      const [millisecondsTime, sequenceNumber] = entryId.split("-");
      if (millisecondsTime == 0 && sequenceNumber == 0) {
        connection.write(
          "-ERR The ID specified in XADD must be greater than 0-0\r\n"
        );
      }
      if (redis_stream[streamKey].length == 0) {
        if (millisecondsTime == 0 && sequenceNumber == 0) {
          connection.write(
            "-ERR The ID specified in XADD must be greater than 0-0\r\n"
          );
        }
      } else {
        let flag = true;
        const lastElement = redis_stream[streamKey].slice(-1);
        const [lasttime, lastsequence] = lastElement[0].id.split("-");
        if (millisecondsTime < lasttime) flag = false;
        else if (millisecondsTime == lasttime && sequenceNumber <= lastsequence)
          flag = false;
        if (!flag) {
          connection.write(
            "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
          );
          return;
        }
      }
      const entry = { id: entryId };

      for (let i = 8; i < command.length; i += 4) {
        const fieldName = command[i];
        const fieldValue = command[i + 2];
        if (fieldName && fieldValue) {
          entry[fieldName] = fieldValue;
        }
      }
      redis_stream[streamKey].push(entry);
      connection.write(`$${entryId.length}\r\n${entryId}\r\n`);
    } else if (intr == "xrange") {
      const streamKey = command[4];
      let startkey = command[6];
      let endkey = command[8];
      let endkey_copy = endkey;
      if (!startkey.includes("-")) startkey += "-0";
      if (!endkey.includes("-")) endkey += "-18446744073709551615";
      if (!redis_stream[streamKey]) {
        redis_stream[streamKey] = [];
      }

      const stream = redis_stream[streamKey];
      const [startMs, startSequence] = startkey.split("-");

      let [endMs, endSequence] = endkey.split("-");
      if (endkey_copy == "+") {
         console.log(stream.splice(-1)[0]);
        [endMs, endSequence] = stream.s
        lice(-1)[0].id.split('-');
        console.log(endMs, endSequence);
      }
      console.log(endMs, endSequence);
      const result = stream
        .filter((item) => {
          const [itemMs, itemSequence] = item.id.split("-");
          if (
            itemMs < startMs ||
            (itemMs === startMs && itemSequence < (startSequence || "0"))
          ) {
            return false;
          }
          if (
            itemMs > endMs ||
            (itemMs === endMs && endSequence && itemSequence > endSequence)
          ) {
            return false;
          }
          return true;
        })
        .map((item) => {
          const fields = Object.entries(item)
            .filter(([key]) => key !== "id")
            .flat();
          return [item.id, fields];
        });

      let response = `*${result.length}\r\n`;
      for (const [id, fields] of result) {
        response += `*2\r\n`;
        response += `$${id.length}\r\n${id}\r\n`;
        response += `*${fields.length}\r\n`;
        for (const field of fields) {
          response += `$${field.length}\r\n${field}\r\n`;
        }
      }
      connection.write(response);
    }
  });
});

server.listen(6379, "127.0.0.1");

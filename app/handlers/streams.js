import { streamSequenceMap, redisStream } from "../state/store.js";

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

function xadd_handler(command, connection) {
  const streamKey = command[4];
  let entryId = command[6];
  entryId = generateStreamId(entryId);
  if (!redisStream[streamKey]) {
    redisStream[streamKey] = [];
  }

  const [millisecondsTime, sequenceNumber] = entryId.split("-");
  if (millisecondsTime == 0 && sequenceNumber == 0) {
    connection.write(
      "-ERR The ID specified in XADD must be greater than 0-0\r\n"
    );
  }
  if (redisStream[streamKey].length == 0) {
    if (millisecondsTime == 0 && sequenceNumber == 0) {
      connection.write(
        "-ERR The ID specified in XADD must be greater than 0-0\r\n"
      );
    }
  } else {
    let flag = true;
    const lastElement = redisStream[streamKey].slice(-1);
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
  redisStream[streamKey].push(entry);
  connection.write(`$${entryId.length}\r\n${entryId}\r\n`);
}

function x_range_handler(startkey, endkey, command, connection) {
  const streamKey = command[4];
  let endkey_copy = endkey;
  if (!startkey.includes("-")) startkey += "-0";
  if (!endkey.includes("-")) endkey += "-18446744073709551615";
  if (!redisStream[streamKey]) {
    redisStream[streamKey] = [];
  }

  const stream = redisStream[streamKey];
  const [startMs, startSequence] = startkey.split("-");

  let [endMs, endSequence] = endkey.split("-");
  if (endkey_copy == "+") {
    const last = stream.slice(-1)[0];
    if (last) {
      [endMs, endSequence] = last.id.split("-");
    } else {
      endMs = "18446744073709551615";
      endSequence = "18446744073709551615";
    }
  }

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

function xread_handler(command, connection) {
  // Find STREAMS keyword
  let streamsIndex = -1;
  for (let i = 0; i < command.length; i++) {
    if (command[i] && command[i].toLowerCase() === "streams") {
      streamsIndex = i;
      break;
    }
  }
  const stream_count = (command.length - 6) / 4; /*why lets say array  is 
  ['*4','$5','XREAD','$7','streams', '$6', 'orange','$3','0-0','' ]   
     now the left byte diviedd bey 4 because length-6 /2 for ids and length-6 /2 is streamkey    */
  let stream_key_index = streamsIndex + 2;
  let stream_keys = [];
  for (let i = 0; i < stream_count; i++) {
    stream_keys.push(command[stream_key_index]);
    stream_key_index += 2;
  }
  
  // Ensure all streams exist
  for (let i = 0; i < stream_count; i++) {
    const streamKey = stream_keys[i];
    if (!redisStream[streamKey]) {
      redisStream[streamKey] = [];
    }
  }
  
  let offset_ids = 5 + (stream_count * 2) + 1;
  console.log(stream_keys);
  
  // Build array of results first
  const results = [];
  for (let i = 0; i < stream_count; i++) {
    const streamKey = stream_keys[i];
    const stream = redisStream[streamKey];
    const startId = command[offset_ids];
    offset_ids += 2;
    const [startMs, startSequence] = startId.split("-");
    
    const filteredEntries = stream.filter((item) => {
      const [itemMs, itemSequence] = item.id.split("-");
      if (itemMs > startMs) return true;
      if (itemMs === startMs && itemSequence > startSequence) return true;
      return false;
    });
    
    // Map entries to array format [id, [field1, value1, field2, value2, ...]]
    const entriesArray = filteredEntries.map((entry) => {
      const fields = Object.entries(entry)
        .filter(([key]) => key !== "id")
        .flat();
      return [entry.id, fields];
    });
    
    results.push([streamKey, entriesArray]);
  }
  

  let response = `*${results.length}\r\n`; 
  
  for (const [streamKey, entriesArray] of results) {
    response += `*2\r\n`; // Stream has 2 parts: key and entries
    response += `$${streamKey.length}\r\n${streamKey}\r\n`;
    response += `*${entriesArray.length}\r\n`; // Number of entries

    for (const [id, fields] of entriesArray) {
      response += `*2\r\n`; // Entry has 2 parts: id and fields
      response += `$${id.length}\r\n${id}\r\n`;
      response += `*${fields.length}\r\n`;
      for (const field of fields) {
        response += `$${field.length}\r\n${field}\r\n`;
      }
    }
  }
  
  connection.write(response);
}

export { generateStreamId, xadd_handler, x_range_handler, xread_handler };

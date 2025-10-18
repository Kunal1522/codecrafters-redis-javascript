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

function xadd_handler(command, connection,blocked_streams) {
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
    return;
  }
  if (redisStream[streamKey].length == 0) {
    if (millisecondsTime == 0 && sequenceNumber == 0) {
      connection.write(
        "-ERR The ID specified in XADD must be greater than 0-0\r\n"
      );
      return;
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

  // Check if there are any blocked XREAD clients waiting for this stream
  if (blocked_streams[streamKey] && blocked_streams[streamKey].length > 0) {
    const client = blocked_streams[streamKey].shift();
    
    // Get the newly added entry from the stream (it's the last one)
    const newEntry = redisStream[streamKey][redisStream[streamKey].length - 1];
    
    // Build response with just the newly added entry
    const fields = Object.entries(newEntry)
      .filter(([k]) => k !== "id")
      .flat();
    
    // Build RESP response for the single new entry
    let response = `*1\r\n`; // 1 stream
    response += `*2\r\n`; // Stream has 2 parts: key and entries
    response += `$${streamKey.length}\r\n${streamKey}\r\n`;
    response += `*1\r\n`; // 1 entry (the new one)
    response += `*2\r\n`; // Entry has 2 parts: id and fields
    response += `$${newEntry.id.length}\r\n${newEntry.id}\r\n`;
    response += `*${fields.length}\r\n`;
    for (const field of fields) {
      response += `$${field.length}\r\n${field}\r\n`;
    }
    
    client.connection.write(response);
  }
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

function xread_handler(command, connection, blocked_streams) {
  // Find BLOCK parameter
  let blockset = 0;
  let timeout = null;
  let blockIndex = -1;
  
  for (let i = 0; i < command.length; i++) {
    if (command[i] && command[i].toLowerCase() === "block") {
      blockIndex = i;
      timeout = Number(command[i + 2]); // Next element after "block" (skip length indicator)
      blockset = 2; // Adjust offsets for BLOCK <timeout>
      break;
    }
  }
  
  // Find STREAMS keyword
  let streamsIndex = -1;
  for (let i = 0; i < command.length; i++) {
    if (command[i] && command[i].toLowerCase() === "streams") {
      streamsIndex = i;
      break;
    }
  }
  
  const stream_count = (command.length - 6 - blockset) / 4;
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
  
  let offset_ids = 5 + stream_count * 2 + 1 + blockset;
  
  // Extract start IDs for each stream
  let start_ids = [];
  let temp_offset = offset_ids;
  for (let i = 0; i < stream_count; i++) {
    start_ids.push(command[temp_offset]);
    temp_offset += 2;
  }

  // Build array of results first
  const results = [];
  let hasEntries = false;

  for (let i = 0; i < stream_count; i++) {
    const streamKey = stream_keys[i];
    const stream = redisStream[streamKey];
    const startId = start_ids[i];
    const [startMs, startSequence] = startId.split("-");

    const filteredEntries = stream.filter((item) => {
      const [itemMs, itemSequence] = item.id.split("-");
      if (itemMs > startMs) return true;
      if (itemMs === startMs && itemSequence > startSequence) return true;
      return false;
    });

    if (filteredEntries.length > 0) {
      hasEntries = true;
      // Only include streams with new entries
      const entriesArray = filteredEntries.map((entry) => {
        const fields = Object.entries(entry)
          .filter(([key]) => key !== "id")
          .flat();
        return [entry.id, fields];
      });
      results.push([streamKey, entriesArray]);
    }
  }

  // If blocking is enabled and no entries are available, block the client
  if (timeout !== null && !hasEntries) {
    const client = {
      connection: connection,
      streamKey: stream_keys[0], // For now, support single stream blocking
      startId: start_ids[0]
    };
    
    // Add client to the blocked queue for this specific stream
    const streamKey = stream_keys[0];
    if (!blocked_streams[streamKey]) {
      blocked_streams[streamKey] = [];
    }
    blocked_streams[streamKey].push(client);
    
    // If timeout is not 0, set a timer to unblock with null response
    if (timeout !== 0) {
      setTimeout(() => {
        // Check if this client is still in the blocked list for this stream
        const idx = blocked_streams[streamKey]?.indexOf(client);
        if (idx !== undefined && idx !== -1) {
          blocked_streams[streamKey].splice(idx, 1);
          connection.write(`*-1\r\n`);
        }
      }, timeout);
    }
    // If timeout is 0, block indefinitely (don't send response now)
    return;
  }

  // Build and send RESP response
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

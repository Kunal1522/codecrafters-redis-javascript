function expiry_checker(command, rkvp) {
  const expiryType = command[3].toLowerCase();
  const expiryValue = command[4];
  const multiplier = expiryType === "ex" ? 1000 : 1;
  
  setTimeout(() => {
    rkvp.set(command[1], "ille_pille_kille");
  }, expiryValue * multiplier);
}

function writeToConnection(connection, data, command, serverConfig, replicatableCommands) {
  if (serverConfig.role === "slave" && replicatableCommands.includes(command?.toUpperCase())) {
    return;
  }
  connection.write(data);
}

function parseMultipleCommands(data) {
  const buf = Buffer.isBuffer(data) ? data : Buffer.from(data);
  const commands = [];
  let pos = 0;

  while (pos < buf.length) {
    // find next '*' that looks like an array header
    let starPos = buf.indexOf("*", pos);
    if (starPos === -1) break;
    let lineEnd = buf.indexOf("\r\n", starPos);
    if (lineEnd === -1) break;
    const header = buf.slice(starPos + 1, lineEnd).toString();
    if (!/^[0-9]+$/.test(header)) {
      pos = starPos + 1;
      continue;
    }
    const arrayLen = parseInt(header, 10);
    pos = lineEnd + 2;

    const parts = [];
    let valid = true;
    for (let i = 0; i < arrayLen; i++) {
      if (pos >= buf.length || buf[pos] !== 36) { // '$'
        valid = false;
        break;
      }
      lineEnd = buf.indexOf("\r\n", pos);
      if (lineEnd === -1) { valid = false; break; }
      const bulkLen = parseInt(buf.slice(pos + 1, lineEnd).toString(), 10);
      pos = lineEnd + 2;
      if (pos + bulkLen > buf.length) { valid = false; break; }
      const valueBuf = buf.slice(pos, pos + bulkLen);
      parts.push(valueBuf.toString());
      pos += bulkLen;
      if (buf[pos] === 13 && buf[pos + 1] === 10) pos += 2; // skip CRLF
    }

    if (!valid) break;
    commands.push(parts);
  }

  return commands;
}

function getCommandByteSize(data) {
  const dataStr = typeof data === 'string' ? data : data.toString();
  let pos = 0;
  
  while (pos < dataStr.length && dataStr[pos] !== '*') pos++;
  if (pos >= dataStr.length) return 0;
  
  const commandStart = pos;
  let lineEnd = dataStr.indexOf('\r\n', pos);
  if (lineEnd === -1) return 0;
  
  const arrayLength = parseInt(dataStr.substring(pos + 1, lineEnd));
  pos = lineEnd + 2;
  
  for (let i = 0; i < arrayLength; i++) {
    if (pos >= dataStr.length || dataStr[pos] !== '$') break;
    
    lineEnd = dataStr.indexOf('\r\n', pos);
    if (lineEnd === -1) break;
    
    const bulkLength = parseInt(dataStr.substring(pos + 1, lineEnd));
    pos = lineEnd + 2 + bulkLength;
    
    if (pos + 1 < dataStr.length && dataStr.substring(pos, pos + 2) === '\r\n') {
      pos += 2;
    }
  }
  
  return pos - commandStart;
}

export { expiry_checker, writeToConnection, parseMultipleCommands, getCommandByteSize };

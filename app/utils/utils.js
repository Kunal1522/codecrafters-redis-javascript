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


export { expiry_checker, writeToConnection, parseMultipleCommands};
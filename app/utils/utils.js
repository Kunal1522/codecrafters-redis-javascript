function expiry_checker(command, rkvp) {
  const expiryType = command[8].toLowerCase();
  const expiryValue = command[10];
  const multiplier = expiryType === "ex" ? 1000 : 1;
  
  setTimeout(() => {
    rkvp.set(command[4], "ille_pille_kille");
  }, expiryValue * multiplier);
}

function writeToConnection(connection, data, command, serverConfig, replicatableCommands) {
  if (serverConfig.role === "slave" && replicatableCommands.includes(command?.toUpperCase())) {
    return;
  }
  connection.write(data);
}

function parseMultipleCommands(data) {
  const dataStr = data.toString();
  const commands = [];
  let pos = 0;
  
  while (pos < dataStr.length) {
    while (pos < dataStr.length && dataStr[pos] !== '*') pos++;
    if (pos >= dataStr.length) break;
    
    const commandStart = pos;
    if (dataStr[pos] !== '*') break;
    
    let lineEnd = dataStr.indexOf('\r\n', pos);
    if (lineEnd === -1) break;
    
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
    
    const commandData = dataStr.substring(commandStart, pos);
    const parts = commandData.split('\r\n');
    
    if (parts.length > 2 && parts[0].startsWith('*')) {
      commands.push(parts);
    }
  }
  
  return commands;
}

export { expiry_checker, writeToConnection, parseMultipleCommands };

function expiry_checker(command,rkvp) {
  if (command[8].toLowerCase() == "px") {
    const expiry = command[10];
    console.log(expiry);
    setTimeout(() => {
      rkvp.set(command[4], `ille_pille_kille`);
    }, expiry);
  } else if (command[8].toLowerCase() == "ex") {
    const expiry = command[10];
    setTimeout(() => {
      rkvp.set(command[4], `ille_pille_kille`);
    }, expiry * 1000);
  }
}

/**
 * Conditionally writes to connection based on server role and command type.
 * Replicas should not send responses for replicatable commands.
 * @param {Object} connection - The socket connection
 * @param {string} data - The data to write
 * @param {string} command - The command being executed
 * @param {Object} serverConfig - The server configuration
 * @param {Array} replicatableCommands - List of commands that can be replicated
 */
function writeToConnection(connection, data, command, serverConfig, replicatableCommands) {
  // Skip writing response if this is a replica executing a replicatable command
  if (serverConfig.role === "slave" && replicatableCommands.includes(command?.toUpperCase())) {
    console.log(`[REPLICA] Silently executing ${command}, skipping response`);
    return;
  }
  connection.write(data);
}

export {expiry_checker, writeToConnection};

function multi_handler(command, connection,taskqueue) {
  let task = {
    connection: connection,
    command: command,
  };
  taskqueue.push(task);
  connection.write(`+\r\n`);
}
export { multi_handler };

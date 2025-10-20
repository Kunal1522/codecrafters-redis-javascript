function multi_handler(command, connection, taskqueue) {
  let task = {
    connection: connection,
    command: command,
  };
  taskqueue.push(task);
  connection.write(`+QUEUED\r\n`);
}
function exec_hanlder(command, connection, taskqueue, is_multi_active) {
  if (!is_multi_active) {
    connection.write(`-ERR EXEC without MULTI\r\n`);
    return;
  }
  console.log(taskqueue.empty());
  if (taskqueue.empty()) {
    connection.write("*0\r\n");
  }
  is_multi_active = false;
}
export { multi_handler, exec_hanlder };

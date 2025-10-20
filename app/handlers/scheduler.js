function multi_handler(command, connection,taskqueue) {
  let task = {
    connection: connection,
    command: command,
  };
  taskqueue.push(task);
  connection.write(`+\r\n`);
}
function exec_hanlder(command,connection,taskqueue,is_multi_active)
{
   if(!is_multi_active)
   {
    connection.write(`-ERR EXEC without MULTI\r\n`);
    return ;
   }
}
export { multi_handler ,exec_hanlder};

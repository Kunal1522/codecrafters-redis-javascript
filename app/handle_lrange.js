function lrange_handler(command,redis_list,connection) {
  const key = command[4];
  const start = Number(command[6]);
  const stop = Number(command[8]);
  if(start<0)
     start=redis_list[key].length+start;
    if(stop<0)
        stop=redis_list[key].length+stop;

  if (!redis_list[key]) {
    connection.write("*0\r\n");
    return;
  }
  const list = redis_list[key];

  if (start >= list.length || start > stop) {
    connection.write("*0\r\n");
    return;
  }
  const end = Math.min(stop, list.length - 1);
  connection.write(`*${end - start + 1}\r\n`);
  for (let i = start; i <= end; i++) {
    const val = list[i];
    connection.write(`$${val.length}\r\n${val}\r\n`);
  }
}
 
export {lrange_handler};

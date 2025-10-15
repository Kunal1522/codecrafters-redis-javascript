function lrange_handler(command,redis_list,connection) {
  const key = command[4];
  const start = Number(command[6]);
  const stop = Number(command[8]);
  // if list doesn't exist -> empty array
  if (!redis_list[key]) {
    connection.write("*0\r\n");
    return;
  }
  const list = redis_list[key];

  // if start >= length or start > stop -> empty array
  if (start >= list.length || start > stop) {
    connection.write("*0\r\n");
    return;
  }

  // clamp stop to the list's length - 1
  const end = Math.min(stop, list.length - 1);

  // send array header
  connection.write(`*${end - start + 1}\r\n`);

  // send elements
  for (let i = start; i <= end; i++) {
    const val = list[i];
    connection.write(`$${val.length}\r\n${val}\r\n`);
  }
}
 
export {lrange_handler};

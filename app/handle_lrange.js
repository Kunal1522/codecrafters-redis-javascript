function lrange_handler(command,redis_list,connection) {
  const key = command[4];
  let start = Number(command[6]);
  let stop = Number(command[8]);
  if(start<0)
     start=redis_list[key].length+start;
    if(stop<0)
        stop=redis_list[key].length+stop;
 start = Math.max(start, 0);
 stop = Math.max(stop, 0);
 start = Math.min(start, redis_list[key] ? redis_list[key].length - 1 : 0);
 stop = Math.min(stop, redis_list[key] ? redis_list[key].length - 1 : 0);
  if (!redis_list[key]) {
    connection.write("*0\r\n");
    return;
  }
  const list = redis_list[key];
const end = Math.min(stop, list.length - 1);
 
  if (start >= list.length || start > stop || stop < 0 || list.length == 0 || start < 0) {
    connection.write("*0\r\n");
    return;
  }
  
  connection.write(`*${end - start + 1}\r\n`);
  for (let i = start; i <= end; i++) {
    const val = list[i];
    connection.write(`$${val.length}\r\n${val}\r\n`);
  }
}
 
export {lrange_handler};

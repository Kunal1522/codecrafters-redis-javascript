function lrange_handler(command, redis_list, connection) {
  const key = command[4];
  let start = Number(command[6]);
  let stop = Number(command[8]);
  if (start < 0) start = redis_list[key].length + start;
  if (stop < 0) stop = redis_list[key].length + stop;
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
  if (
    start >= list.length ||
    start > stop ||
    stop < 0 ||
    list.length == 0 ||
    start < 0
  ) {
    connection.write("*0\r\n");
    return;
  }

  connection.write(`*${end - start + 1}\r\n`);
  for (let i = start; i <= end; i++) {
    const val = list[i];
    connection.write(`$${val.length}\r\n${val}\r\n`);
  }

}

function lpop_handler(command, redis_list, connection) {
   const key = command[4];
      if (command.length > 6) {
        const element_to_pop = command[6];
        let elements_remove = [];
        for (let i = 0; i < element_to_pop; i++) {
          const top_most = redis_list[key].shift();
          if (top_most == undefined) {
            break;
          }
          elements_remove.push(top_most);
        }
        connection.write("*" + elements_remove.length + "\r\n");
        for (let i = 0; i < elements_remove.length; i++) {
          connection.write(
            "$" +
              elements_remove[i].length +
              "\r\n" +
              elements_remove[i] +
              "\r\n"
          );
        }
      } else {
        if (redis_list[key].length == 0) connection.write(`$-1\r\n`);
        else {
          const top_most = redis_list[key].shift();
          connection.write("$" + top_most.length + "\r\n" + top_most + "\r\n");
        }
      }
}
function blop_handler(command,redis_list,blop_connections,connection)
{
   const key = command[4];
        console.log(key);
         if (!blop_connections[key]) {
        blop_connections[key] = [];
      }
       blop_connections[key].push(connection); 
         console.log("time of blop",performance.now());
      const timeout = command.length > 6 ? Number(command[6]) * 1000 : null;
       console.log(connection);
      if (timeout != 0) {
        setTimeout(() => {    
         const  top_connection=blop_connections[key].shift(); 
          const top_most =
            redis_list[key] && redis_list[key].length > 0
              ? redis_list[key].shift():null;
         if (top_most == null) top_connection.write(`*-1\r\n`);
        else
         {
          top_connection.write(`*2\r\n$${key.length}\r\n${key}\r\n$${top_most.length}\r\n${top_most}\r\n`);
         }        
        }, timeout);
      }
}

function rpush_handler(command,redis_list,blop_connections,connection)
{
      const key = command[4];
      if (!redis_list[key]) {
        redis_list[key] = [];
      }
      for (let i = 6; i < command.length; i += 2) {
        redis_list[key].push(command[i]);
      }
      connection.write(":" + redis_list[key].length + "\r\n");
      if (!blop_connections[key]) {
        blop_connections[key] = [];
      }
      console.log("time of rpush", performance.now());
      console.log("blopconnectionlenght", blop_connections[key].length);
      if (blop_connections[key].length > 0) {
        const top_connection = blop_connections[key].shift();
        const top_most =
          redis_list[key] && redis_list[key].length > 0
            ? redis_list[key].shift()
            : null;
        console.log("TOPMOSTELEMENT", top_most);
        console.log("top connection", top_connection);
        if (top_most == null) {
          top_connection.write(`*-1\r\n`);
        } else {
          top_connection.write(
            `*2\r\n$${key.length}\r\n${key}\r\n$${top_most.length}\r\n${top_most}\r\n`
          );
        }
      }
}
export { lrange_handler, lpop_handler,blop_handler,rpush_handler};

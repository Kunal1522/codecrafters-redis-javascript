import net from "net";
import { serverConfig } from "../config.js";
console.log("server configuration", serverConfig);
function createMasterConnection() {
  const slave_connection = net.createConnection(
    { port: serverConfig.master_port, host: "127.0.0.1" },
    () => {
      console.log("connected to master");
      slave_connection.write(`*1\r\n$4\r\nPING\r\n`);
      setTimeout(() => {
        connection.write(
          "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n"
        );
      }, 100);
      setTimeout(() => {
        connection.write(
          "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
        );
      }, 100);
      setTimeout(() => {
        connection.write(`*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n`);
      }, 1000);
    }
  );
  return connection;
}
//GAJNI HU MAI ..sb bhool jaata....
//the problem is here we have the following config 
//first the tester in codecrafters requests the slave connection but the issue is i can't put the 
// slave connection inside the server connection in main.js because the tester expects the slave connection before even connecting to my server 
//so my trick is to actually implement the slave connection using new socket who pretends that it is replica and sends request but the issue is when master(tester here ) sends data it sends to the port that i defined in main.js ..... so i am using that connection obj to send any data back ....
function master_handler(connection)
{
     connection.write()
}
export { createMasterConnection,master_handler};


let port = 6379;
let serverConfig={
  port:6379,
  role:"master",
  master_replid:"8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
  master_repl_offset:0
};
const args = process.argv;
const portIndex = args.indexOf("--port");
if (portIndex !== -1 && args[portIndex + 1]) {
  serverConfig.port = parseInt(args[portIndex + 1], 10);
  
}
const replicaIndex=args.indexOf("--replicaof");
if(replicaIndex!==-1 && args[replicaIndex+1])
{
  serverConfig.role="slave";
}
export  { serverConfig };
let serverConfig = {
  port: 6379,
  role: "master",
  master_replid: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
  master_repl_offset: 0,
  replica_offset: 0,
  master_host: undefined,
  master_port: undefined,
  master_replica_connection: undefined
};

const args = process.argv;
console.log(args);

const portIndex = args.indexOf("--port");
if (portIndex !== -1 && args[portIndex + 1]) {
  serverConfig.port = parseInt(args[portIndex + 1], 10);
}

const replicaIndex = args.indexOf("--replicaof");
if (replicaIndex !== -1 && args[replicaIndex + 1]) {
  serverConfig.role = "slave";
  if (args[replicaIndex + 1].includes('localhost')) {
    const [hostname, port] = args[replicaIndex + 1].split(' ');
    serverConfig.master_host = hostname;
    serverConfig.master_port = parseInt(port, 10);
  } 
}

export { serverConfig };

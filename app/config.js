let port = 6379;
const args = process.argv;
console.log(args);
const portIndex = args.indexOf('--port');
if (portIndex != -1 && args[portIndex + 1]) {
  port = parseInt(args[portIndex], 10);
}
console.log(port);
export {port};
import { MyQueue  } from "../utils/queue.js";
const redisKeyValuePair = new Map();
const redisList = {};
const blpopConnections = {};
const redisStream = {};
const streamSequenceMap = new Map();
const blocked_streams = {};

const REPLICATABLE_COMMANDS = [
  'SET',
  'DEL',
  'INCR',
  'INCRBY',
  'ZADD',
  'ZREM',
  'XADD',
  'RPUSH',
  'LPUSH',
  'RPOP',
  'LPOP',
  'BLPOP',
];
const replicas_connected=new Set();
export { redisKeyValuePair, redisList, blpopConnections, redisStream, streamSequenceMap,blocked_streams,REPLICATABLE_COMMANDS,replicas_connected};

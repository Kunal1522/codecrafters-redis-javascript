import { MyQueue } from "../utils/queue.js";
const redisKeyValuePair = new Map();
const redisList = {};
const blpopConnections = {};
const redisStream = {};
const streamSequenceMap = new Map();
const blocked_streams = {};
const REPLICATABLE_COMMANDS = [
  "SET",
  "DEL",
  "INCR",
  "INCRBY",
  "ZADD",
  "ZREM",
  "XADD",
  "RPUSH",
  "LPUSH",
  "RPOP",
  "LPOP",
  "BLPOP",
];
const subsriber_commannds = [
  "SUBSCRIBE",
  "UNSUBSCRIBE",
  "PSUBSCRIBE",
  "PUNSUBSCRIBE",
  "PING",
  "QUIT",
];
const replicas_connected = new Set();
const master_offset = new Map();
const pendingWaitRequest = {
  active: false,
  clientConnection: null,
  numRequired: 0,
  timeout: 0,
  ackedReplicas: new Set(),
  timeoutId: null,
  replicaExpectedOffsets: new Map(),
};
const subchannel = new Map();
const redisSortedSet = new Map();
export {
  redisKeyValuePair,
  redisList,
  blpopConnections,
  redisStream,
  streamSequenceMap,
  blocked_streams,
  REPLICATABLE_COMMANDS,
  replicas_connected,
  master_offset,
  pendingWaitRequest,
  subsriber_commannds,
  subchannel,
  redisSortedSet,
};

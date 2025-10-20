import { MyQueue  } from "../utils/queue.js";
const redisKeyValuePair = new Map();
const redisList = {};
const blpopConnections = {};
const redisStream = {};
const streamSequenceMap = new Map();
const blocked_streams = {};

export { redisKeyValuePair, redisList, blpopConnections, redisStream, streamSequenceMap,blocked_streams};

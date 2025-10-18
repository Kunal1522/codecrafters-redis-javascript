const redisKeyValuePair = new Map();
const redisList = {};
const blpopConnections = {};
const redisStream = {};
const streamSequenceMap = new Map();
const blocked_streams = {}; // Changed from array to object keyed by stream name
export { redisKeyValuePair, redisList, blpopConnections, redisStream, streamSequenceMap,blocked_streams };

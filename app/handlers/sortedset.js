import { redisSortedSet } from "../state/store.js";
import { SkipList } from "../data_structures/skiplist.js";

function zadd_handler(command, connection) {
  const key = command[1];
  const score = parseFloat(command[2]);
  const member = command[3];

  if (!redisSortedSet.has(key)) {
    redisSortedSet.set(key, {
      skiplist: new SkipList(),
      map: new Map()
    });
  }
  const { skiplist, map } = redisSortedSet.get(key);
  const existed = map.has(member);

  if (existed) {
    const oldScore = map.get(member);
    skiplist.delete(oldScore, member);
  }
  skiplist.insert(score, member);
  map.set(member, score);
  connection.write(existed ? `:0\r\n` : `:1\r\n`);
}

function zrank_handler(command, connection) {
  const key = command[1];
  const member = command[2];

  if (!redisSortedSet.has(key)) {
    connection.write(`$-1\r\n`);
    return;
  }

  const { skiplist, map } = redisSortedSet.get(key);

  if (!map.has(member)) {
    connection.write(`$-1\r\n`);
    return;
  }
  const score = map.get(member);
  const rank = skiplist.getRank(score, member);
  connection.write(`:${rank}\r\n`);
}

function zrange_handler(command, connection) {
  const key = command[1];
  let start = parseInt(command[2]);
  let stop = parseInt(command[3]);

  if (!redisSortedSet.has(key)) {
    connection.write(`*0\r\n`);
    return;
  }

  const { skiplist } = redisSortedSet.get(key);
  const result = skiplist.getRange(start, stop);

  connection.write(`*${result.length}\r\n`);
  for (const item of result) {
    connection.write(`$${item.member.length}\r\n${item.member}\r\n`);
  }
}

function zcard_handler(command, connection) {
  const key = command[1];

  if (!redisSortedSet.has(key)) {
    connection.write(`:0\r\n`);
    return;
  }

  const { skiplist } = redisSortedSet.get(key);
  connection.write(`:${skiplist.length}\r\n`);
}

function zscore_handler(command, connection) {
  const key = command[1];
  const member = command[2];

  if (!redisSortedSet.has(key)) {
    connection.write(`$-1\r\n`);
    return;
  }

  const { map } = redisSortedSet.get(key);

  if (!map.has(member)) {
    connection.write(`$-1\r\n`);
    return;
  }

  const score = map.get(member).toString();
  connection.write(`$${score.length}\r\n${score}\r\n`);
}

function zrem_handler(command, connection) {
  const key = command[1];
  const member = command[2];

  if (!redisSortedSet.has(key)) {
    connection.write(`:0\r\n`);
    return;
  }

  const { skiplist, map } = redisSortedSet.get(key);

  if (!map.has(member)) {
    connection.write(`:0\r\n`);
    return;
  }

  const score = map.get(member);
  skiplist.delete(score, member);
  map.delete(member);
  connection.write(`:1\r\n`);
}

export {
  zadd_handler,
  zrank_handler,
  zrange_handler,
  zcard_handler,
  zscore_handler,
  zrem_handler,
};
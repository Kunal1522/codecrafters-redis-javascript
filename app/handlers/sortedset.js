import { redisSortedSet } from "../state/store.js";
import { SortedSet } from "../data_structures/sortedset.js";

function zadd_handler(command, connection) {
  const key = command[1];
  const score = parseFloat(command[2]);
  const member = command[3];

  if (!redisSortedSet.has(key)) {
    redisSortedSet.set(key, new SortedSet());
  }
  const sortedSet = redisSortedSet.get(key);
  const result = sortedSet.add(score, member);
  connection.write(`:${result}\r\n`);
}

function zrank_handler(command, connection) {
  const key = command[1];
  const member = command[2];

  if (!redisSortedSet.has(key)) {
    connection.write(`$-1\r\n`);
    return;
  }

  const sortedSet = redisSortedSet.get(key);
  const rank = sortedSet.getRank(member);

  if (rank === -1) {
    connection.write(`$-1\r\n`);
  } else {
    connection.write(`:${rank}\r\n`);
  }
}

function zrange_handler(command, connection) {
  const key = command[1];
  let start = parseInt(command[2]);
  let stop = parseInt(command[3]);

  if (!redisSortedSet.has(key)) {
    connection.write(`*0\r\n`);
    return;
  }

  const sortedSet = redisSortedSet.get(key);
  const result = sortedSet.getRange(start, stop);

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

  const sortedSet = redisSortedSet.get(key);
  const count = sortedSet.getCardinality();
  connection.write(`:${count}\r\n`);
}

function zscore_handler(command, connection) {
  const key = command[1];
  const member = command[2];

  if (!redisSortedSet.has(key)) {
    connection.write(`$-1\r\n`);
    return;
  }

  const sortedSet = redisSortedSet.get(key);
  const score = sortedSet.getScore(member);

  if (score === null) {
    connection.write(`$-1\r\n`);
  } else {
    const scoreStr = score.toString();
    connection.write(`$${scoreStr.length}\r\n${scoreStr}\r\n`);
  }
}

function zrem_handler(command, connection) {
  const key = command[1];
  const member = command[2];

  if (!redisSortedSet.has(key)) {
    connection.write(`:0\r\n`);
    return;
  }

  const sortedSet = redisSortedSet.get(key);
  const result = sortedSet.remove(member);
  connection.write(`:${result}\r\n`);
}

export {
  zadd_handler,
  zrank_handler,
  zrange_handler,
  zcard_handler,
  zscore_handler,
  zrem_handler,
};
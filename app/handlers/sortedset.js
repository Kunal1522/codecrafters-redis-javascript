import { redisSortedSet } from "../state/store.js";

function getSortedItems(sortedSet) {
  const items = Array.from(sortedSet.entries()).map(([member, score]) => ({
    score,
    member
  }));
  
  items.sort((a, b) => {
    if (a.score !== b.score) {
      return a.score - b.score;
    }
    return a.member.localeCompare(b.member);
  });
  
  return items;
}

function zadd_handler(command, connection) {
  const key = command[1];
  const score = parseFloat(command[2]);
  const member = command[3];
  
  if (!redisSortedSet.has(key)) {
    redisSortedSet.set(key, new Map());
  }
  
  const sortedSet = redisSortedSet.get(key);
  const existed = sortedSet.has(member);
  
  sortedSet.set(member, score);
  
  if (existed) {
    connection.write(`:0\r\n`);
  } else {
    connection.write(`:1\r\n`);
  }
}

function zrank_handler(command, connection) {
  const key = command[1];
  const member = command[2];

  if (!redisSortedSet.has(key)) {
    connection.write(`$-1\r\n`);
    return;
  }

  const sortedSet = redisSortedSet.get(key);
  
  if (!sortedSet.has(member)) {
    connection.write(`$-1\r\n`);
    return;
  }

  const allItems = getSortedItems(sortedSet);
  const rank = allItems.findIndex(item => item.member === member);
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

  const sortedSet = redisSortedSet.get(key);
  const allItems = getSortedItems(sortedSet);
  const length = allItems.length;

  if (start < 0) {
    start = Math.max(0, length + start);
  }
  if (stop < 0) {
    stop = Math.max(0, length + stop);
  }

  if (start >= length || start > stop) {
    connection.write(`*0\r\n`);
    return;
  }

  const endIndex = Math.min(stop, length - 1);
  const result = allItems.slice(start, endIndex + 1);

  connection.write(`*${result.length}\r\n`);
  for (const item of result) {
    connection.write(`$${item.member.length}\r\n${item.member}\r\n`);
  }
}

export { zadd_handler, zrank_handler, zrange_handler };

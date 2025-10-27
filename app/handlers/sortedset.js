import { SkipList } from "skip-list";
import { redisSortedSet } from "../state/store.js";

function zadd_handler(command, connection) {
  const key = command[1];
  const score = parseFloat(command[2]);
  const member = command[3];
  if (!redisSortedSet.has(key)) {
    redisSortedSet.set(key, {
      skiplist: new SkipList(),
      map: new Map(),
    });
  }
  const { skiplist, map } = redisSortedSet.get(key);
  if (map.has(member)) {
    const oldScore = map.get(member);
    skiplist.remove(oldScore);
    skiplist.insert(score, member);
    map.set(member, score);
    connection.write(`:0\r\n`);
  } else {
    skiplist.insert(score, member);
    map.set(member, score);
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

  const { skiplist, map } = redisSortedSet.get(key);
  
  if (!map.has(member)) {
    connection.write(`$-1\r\n`);
    return;
  }

  const score = map.get(member);
  const allItems = [];
  
  let current = skiplist.head.forward[0];
  while (current) {
    allItems.push({ score: current.key, member: current.value });
    current = current.forward[0];
  }

  allItems.sort((a, b) => {
    if (a.score !== b.score) {
      return a.score - b.score;
    }
    return a.member.localeCompare(b.member);
  });

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

  const { skiplist, map } = redisSortedSet.get(key);
  const allItems = [];
  
  let current = skiplist.head.forward[0];
  while (current) {
    allItems.push({ score: current.key, member: current.value });
    current = current.forward[0];
  }

  allItems.sort((a, b) => {
    if (a.score !== b.score) {
      return a.score - b.score;
    }
    return a.member.localeCompare(b.member);
  });

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

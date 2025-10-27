import { redisSortedSet } from "../state/store.js";

function zadd_handler(command, connection) {
  const key = command[1];
  const score = parseFloat(command[2]);
  const member = command[3];

  if (!redisSortedSet.has(key)) {
    redisSortedSet.set(key, []);
  }
  const sortedSet = redisSortedSet.get(key);
  const existingIndex = sortedSet.findIndex(item => item.member === member);

  if (existingIndex !== -1) {
    sortedSet[existingIndex].score = score;
    sortedSet.sort((a, b) => a.score - b.score);
    connection.write(`:0\r\n`);
  } else {
    sortedSet.push({ score, member });
    sortedSet.sort((a, b) => a.score - b.score);
    connection.write(`:1\r\n`);
  }
}

export { zadd_handler };

class SkipListNode {
  constructor(score, member, level) {
    this.score = score;
    this.member = member;
    this.forward = new Array(level).fill(null);
    this.span = new Array(level).fill(0);
  }
}

class SkipList {
  constructor() {
    this.maxLevel = 32;
    this.p = 0.25;
    this.level = 1;
    this.head = new SkipListNode(null, null, this.maxLevel);
    this.length = 0;
  }

  randomLevel() {
    let level = 1;
    while (Math.random() < this.p && level < this.maxLevel) {
      level++;
    }
    return level;
  }
  compare(score1, member1, score2, member2) {
    if (score1 !== score2) {
      return score1 - score2;
    }
    return member1.localeCompare(member2);
  }
  insert(score, member) {
    const update = new Array(this.maxLevel);
    const rank = new Array(this.maxLevel).fill(0);
    let current = this.head;

    for (let i = this.level - 1; i >= 0; i--) {
      rank[i] = i === this.level - 1 ? 0 : rank[i + 1];
      
      while (
        current.forward[i] &&
        this.compare(current.forward[i].score, current.forward[i].member, score, member) < 0
      ) {
        rank[i] += current.span[i];
        current = current.forward[i];
      }
      update[i] = current;
    }

    const nodeLevel = this.randomLevel();
    if (nodeLevel > this.level) {
      for (let i = this.level; i < nodeLevel; i++) {
        rank[i] = 0;
        update[i] = this.head;
        update[i].span[i] = this.length;
      }
      this.level = nodeLevel;
    }

    const newNode = new SkipListNode(score, member, nodeLevel);
    for (let i = 0; i < nodeLevel; i++) {
      newNode.forward[i] = update[i].forward[i];
      update[i].forward[i] = newNode;

      newNode.span[i] = update[i].span[i] - (rank[0] - rank[i]);
      update[i].span[i] = rank[0] - rank[i] + 1;
    }

    for (let i = nodeLevel; i < this.level; i++) {
      update[i].span[i]++;
    }

    this.length++;
  }

  delete(score, member) {
    const update = new Array(this.maxLevel);
    let current = this.head;

    for (let i = this.level - 1; i >= 0; i--) {
      while (
        current.forward[i] &&
        this.compare(current.forward[i].score, current.forward[i].member, score, member) < 0
      ) {
        current = current.forward[i];
      }
      update[i] = current;
    }

    current = current.forward[0];
    if (!current || current.score !== score || current.member !== member) {
      return false;
    }

    for (let i = 0; i < this.level; i++) {
      if (update[i].forward[i] === current) {
        update[i].span[i] += current.span[i] - 1;
        update[i].forward[i] = current.forward[i];
      } else {
        update[i].span[i]--;
      }
    }

    while (this.level > 1 && !this.head.forward[this.level - 1]) {
      this.level--;
    }

    this.length--;
    return true;
  }

  getRank(score, member) {
    let rank = 0;
    let current = this.head;
    for (let i = this.level - 1; i >= 0; i--) {
      while (
        current.forward[i] &&
        this.compare(current.forward[i].score, current.forward[i].member, score, member) <= 0
      ) {
        rank += current.span[i];
        current = current.forward[i];
      }
      if (current.member === member && current.score === score) {
        return rank - 1;
      }
    }

    return -1;
  }

  getByRank(rank) {
    if (rank < 0 || rank >= this.length) {
      return null;
    }

    let traversed = 0;
    let current = this.head;

    for (let i = this.level - 1; i >= 0; i--) {
      while (current.forward[i] && traversed + current.span[i] <= rank + 1) {
        traversed += current.span[i];
        current = current.forward[i];
      }

      if (traversed === rank + 1) {
        return current;
      }
    }

    return current;
  }

  getRange(start, stop) {
    if (start < 0) start = Math.max(0, this.length + start);
    if (stop < 0) stop = Math.max(0, this.length + stop);

    if (start >= this.length || start > stop) {
      return [];
    }

    const result = [];
    let current = this.getByRank(start);
    
    for (let i = start; i <= stop && i < this.length && current; i++) {
      result.push({ score: current.score, member: current.member });
      current = current.forward[0];
    }

    return result;
  }
}

export { SkipList };

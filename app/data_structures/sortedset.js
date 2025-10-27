import { SkipList } from "./skiplist.js";

class SortedSet {
  constructor() {
    this.skiplist = new SkipList();
    this.map = new Map();
  }

  add(score, member) {
    const existed = this.map.has(member);

    if (existed) {
      const oldScore = this.map.get(member);
      this.skiplist.delete(oldScore, member);
    }

    this.skiplist.insert(score, member);
    this.map.set(member, score);

    return existed ? 0 : 1;
  }

  getRank(member) {
    if (!this.map.has(member)) {
      return -1;
    }

    const score = this.map.get(member);
    return this.skiplist.getRank(score, member);
  }

  getRange(start, stop) {
    return this.skiplist.getRange(start, stop);
  }

  getCardinality() {
    return this.skiplist.length;
  }

  getScore(member) {
    if (!this.map.has(member)) {
      return null;
    }
    return this.map.get(member);
  }

  remove(member) {
    if (!this.map.has(member)) {
      return 0;
    }

    const score = this.map.get(member);
    this.skiplist.delete(score, member);
    this.map.delete(member);
    return 1;
  }
}

export { SortedSet };

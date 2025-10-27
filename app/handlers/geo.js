import { redisSortedSet } from "../state/store.js";
import { SortedSet } from "../data_structures/sortedset.js";
import { encode } from "../transcoder/encode.js";
import { decode } from "../transcoder/decode.js";

const EARTH_RADIUS_IN_METERS = 6372797.560856;

function degreesToRadians(degrees) {
  return (degrees * Math.PI) / 180;
}

function haversineDistance(lon1, lat1, lon2, lat2) {
  const lat1Rad = degreesToRadians(lat1);
  const lat2Rad = degreesToRadians(lat2);
  const deltaLat = degreesToRadians(lat2 - lat1);
  const deltaLon = degreesToRadians(lon2 - lon1);

  const a =
    Math.sin(deltaLat / 2) * Math.sin(deltaLat / 2) +
    Math.cos(lat1Rad) * Math.cos(lat2Rad) *
    Math.sin(deltaLon / 2) * Math.sin(deltaLon / 2);

  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

  return EARTH_RADIUS_IN_METERS * c;
}

function geoadd_handler(command, connection) {
  const key = command[1];
  const lon = parseFloat(command[2]);
  const lat = parseFloat(command[3]);
  const place = command[4];

  if (
    lon > 180.0 ||
    lon < -180.0 ||
    lat > 85.05112878 ||
    lat < -85.05112878
  ) {
    connection.write(`-ERR invalid latitude longitude pair\r\n`);
    return;
  }

  if (!redisSortedSet.has(key)) {
    redisSortedSet.set(key, new SortedSet());
  }

  const score = Number(encode(lat, lon));
  const sortedSet = redisSortedSet.get(key);
  const result = sortedSet.add(score, place);
  connection.write(`:${result}\r\n`);
}

function geopos_handler(command, connection) {
  const key = command[1];
  const members = command.slice(2);

  connection.write(`*${members.length}\r\n`);

  if (!redisSortedSet.has(key)) {
    for (let i = 0; i < members.length; i++) {
      connection.write(`*-1\r\n`);
    }
    return;
  }

  const sortedSet = redisSortedSet.get(key);

  for (const member of members) {
    const score = sortedSet.getScore(member);
    
    if (score === null) {
      connection.write(`*-1\r\n`);
    } else {
      const { longitude, latitude } = decode(score);
      const lonStr = longitude.toString();
      const latStr = latitude.toString();
      
      connection.write(`*2\r\n`);
      connection.write(`$${lonStr.length}\r\n${lonStr}\r\n`);
      connection.write(`$${latStr.length}\r\n${latStr}\r\n`);
    }
  }
}

function geodist_handler(command, connection) {
  const key = command[1];
  const member1 = command[2];
  const member2 = command[3];
  const unit = command[4] || "m";

  if (!redisSortedSet.has(key)) {
    connection.write(`$-1\r\n`);
    return;
  }

  const sortedSet = redisSortedSet.get(key);
  const score1 = sortedSet.getScore(member1);
  const score2 = sortedSet.getScore(member2);

  if (score1 === null || score2 === null) {
    connection.write(`$-1\r\n`);
    return;
  }

  const pos1 = decode(score1);
  const pos2 = decode(score2);

  let distance = haversineDistance(pos1.longitude, pos1.latitude, pos2.longitude, pos2.latitude);

  const unitMultipliers = {
    m: 1,
    km: 0.001,
    mi: 0.000621371,
    ft: 3.28084
  };

  if (unitMultipliers[unit]) {
    distance *= unitMultipliers[unit];
  }

  const distStr = distance.toString();
  connection.write(`$${distStr.length}\r\n${distStr}\r\n`);
}

function geosearch_handler(command, connection) {
  const key = command[1];

  if (!redisSortedSet.has(key)) {
    connection.write(`*0\r\n`);
    return;
  }

  let centerLon, centerLat, radius, unit;

  for (let i = 2; i < command.length; i++) {
    const arg = command[i].toLowerCase();
    
    if (arg === "fromlonlat") {
      centerLon = parseFloat(command[i + 1]);
      centerLat = parseFloat(command[i + 2]);
      i += 2;
    } else if (arg === "byradius") {
      radius = parseFloat(command[i + 1]);
      unit = command[i + 2].toLowerCase();
      i += 2;
    }
  }

  const unitMultipliers = {
    m: 1,
    km: 1000,
    mi: 1609.34,
    ft: 0.3048
  };

  const radiusInMeters = radius * (unitMultipliers[unit] || 1);

  const sortedSet = redisSortedSet.get(key);
  const allMembers = sortedSet.getRange(0, -1);
  const results = [];

  for (const item of allMembers) {
    const { longitude, latitude } = decode(item.score);
    const distance = haversineDistance(centerLon, centerLat, longitude, latitude);
    
    if (distance <= radiusInMeters) {
      results.push(item.member);
    }
  }

  connection.write(`*${results.length}\r\n`);
  for (const member of results) {
    connection.write(`$${member.length}\r\n${member}\r\n`);
  }
}

export { geoadd_handler, geopos_handler, geodist_handler, geosearch_handler };

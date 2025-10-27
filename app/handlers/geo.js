import { redisSortedSet } from "../state/store.js";
import { SortedSet } from "../data_structures/sortedset.js";

const MIN_LATITUDE = -85.05112878;
const MAX_LATITUDE = 85.05112878;
const MIN_LONGITUDE = -180.0;
const MAX_LONGITUDE = 180.0;
const LATITUDE_RANGE = MAX_LATITUDE - MIN_LATITUDE;
const LONGITUDE_RANGE = MAX_LONGITUDE - MIN_LONGITUDE;
const EARTH_RADIUS_IN_METERS = 6372797.560856;

function spreadInt32ToInt64(v) {
  let result = BigInt(v) & 0xFFFFFFFFn;
  result = (result | (result << 16n)) & 0x0000FFFF0000FFFFn;
  result = (result | (result << 8n)) & 0x00FF00FF00FF00FFn;
  result = (result | (result << 4n)) & 0x0F0F0F0F0F0F0F0Fn;
  result = (result | (result << 2n)) & 0x3333333333333333n;
  result = (result | (result << 1n)) & 0x5555555555555555n;
  return result;
}

function interleave(x, y) {
  const xSpread = spreadInt32ToInt64(x);
  const ySpread = spreadInt32ToInt64(y);
  const yShifted = ySpread << 1n;
  return xSpread | yShifted;
}

function encode(latitude, longitude) {
  const normalizedLatitude = Math.pow(2, 26) * (latitude - MIN_LATITUDE) / LATITUDE_RANGE;
  const normalizedLongitude = Math.pow(2, 26) * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE;
  const latInt = Math.floor(normalizedLatitude);
  const lonInt = Math.floor(normalizedLongitude);
  return Number(interleave(latInt, lonInt));
}

function compactInt64ToInt32(v) {
  v = v & 0x5555555555555555n;
  v = (v | (v >> 1n)) & 0x3333333333333333n;
  v = (v | (v >> 2n)) & 0x0F0F0F0F0F0F0F0Fn;
  v = (v | (v >> 4n)) & 0x00FF00FF00FF00FFn;
  v = (v | (v >> 8n)) & 0x0000FFFF0000FFFFn;
  v = (v | (v >> 16n)) & 0x00000000FFFFFFFFn;
  return Number(v);
}

function decode(geoCode) {
  const code = BigInt(geoCode);
  const y = code >> 1n;
  const x = code;
  const gridLatitudeNumber = compactInt64ToInt32(x);
  const gridLongitudeNumber = compactInt64ToInt32(y);
  
  const gridLatitudeMin = MIN_LATITUDE + LATITUDE_RANGE * (gridLatitudeNumber / Math.pow(2, 26));
  const gridLatitudeMax = MIN_LATITUDE + LATITUDE_RANGE * ((gridLatitudeNumber + 1) / Math.pow(2, 26));
  const gridLongitudeMin = MIN_LONGITUDE + LONGITUDE_RANGE * (gridLongitudeNumber / Math.pow(2, 26));
  const gridLongitudeMax = MIN_LONGITUDE + LONGITUDE_RANGE * ((gridLongitudeNumber + 1) / Math.pow(2, 26));
  
  const latitude = (gridLatitudeMin + gridLatitudeMax) / 2;
  const longitude = (gridLongitudeMin + gridLongitudeMax) / 2;
  
  return { latitude, longitude };
}

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

  const score = encode(lat, lon);
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

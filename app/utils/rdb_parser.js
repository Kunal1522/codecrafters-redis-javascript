import fs from 'fs';
import path from 'path';

function readLengthEncoding(buffer, offset) {
  const firstByte = buffer[offset];
  const type = (firstByte & 0xC0) >> 6;
  
  if (type === 0) {
    return { length: firstByte & 0x3F, bytesRead: 1 };
  } else if (type === 1) {
    const length = ((firstByte & 0x3F) << 8) | buffer[offset + 1];
    return { length, bytesRead: 2 };
  } else if (type === 2) {
    if (firstByte === 0x80) {
      const length = buffer.readUInt32BE(offset + 1);
      return { length, bytesRead: 5 };
    } else if (firstByte === 0x81) {
      const length = Number(buffer.readBigUInt64BE(offset + 1));
      return { length, bytesRead: 9 };
    }
  } else if (type === 3) {
    return { length: firstByte & 0x3F, bytesRead: 1, isSpecial: true };
  }
  
  throw new Error(`Unknown length encoding: ${firstByte.toString(16)}`);
}
function readString(buffer, offset) {
  const { length, bytesRead, isSpecial } = readLengthEncoding(buffer, offset);
  let newOffset = offset + bytesRead;
  if (isSpecial) {
    if (length === 0) {
      const value = buffer.readInt8(newOffset);
      return { value: value.toString(), bytesRead: bytesRead + 1 };
    } else if (length === 1) {
      const value = buffer.readInt16LE(newOffset);
      return { value: value.toString(), bytesRead: bytesRead + 2 };
    } else if (length === 2) {
      const value = buffer.readInt32LE(newOffset);
      return { value: value.toString(), bytesRead: bytesRead + 4 };
    }
  }
  
  const value = buffer.toString('utf8', newOffset, newOffset + length);
  return { value, bytesRead: bytesRead + length };
}

function parseRDB(filePath) {
  if (!fs.existsSync(filePath)) {
    return { keys: new Map() };
  }
  
  const buffer = fs.readFileSync(filePath);
  const keys = new Map();
  let offset = 0;
  
  const magic = buffer.toString('utf8', 0, 5);
  if (magic !== 'REDIS') {
    throw new Error('Invalid RDB file: missing REDIS magic string');
  }
  offset = 9;
  
  while (offset < buffer.length) {
    const opcode = buffer[offset];
    offset++;
    
    if (opcode === 0xFF) {
      break;
    } else if (opcode === 0xFE) {
      const { length: dbIndex, bytesRead } = readLengthEncoding(buffer, offset);
      offset += bytesRead;
    } else if (opcode === 0xFB) {
      const { bytesRead: br1 } = readLengthEncoding(buffer, offset);
      offset += br1;
      const { bytesRead: br2 } = readLengthEncoding(buffer, offset);
      offset += br2;
    } else if (opcode === 0xFA) {
      const { bytesRead: nameBytes } = readString(buffer, offset);
      offset += nameBytes;
      const { bytesRead: valueBytes } = readString(buffer, offset);
      offset += valueBytes;
    } else if (opcode === 0xFC) {
      const expireMs = buffer.readBigUInt64LE(offset);
      offset += 8;
      const valueType = buffer[offset];
      offset++;
      const { value: key, bytesRead: keyBytes } = readString(buffer, offset);
      offset += keyBytes;
      const { value, bytesRead: valueBytes } = readString(buffer, offset);
      offset += valueBytes;
      
      const now = Date.now();
      if (Number(expireMs) > now) {
        keys.set(key, { value, expireAt: Number(expireMs) });
      }
    } else if (opcode === 0xFD) {
      const expireSec = buffer.readUInt32LE(offset);
      offset += 4;
      const valueType = buffer[offset];
      offset++;
      const { value: key, bytesRead: keyBytes } = readString(buffer, offset);
      offset += keyBytes;
      const { value, bytesRead: valueBytes } = readString(buffer, offset);
      offset += valueBytes;
      
      const now = Date.now();
      if (expireSec * 1000 > now) {
        keys.set(key, { value, expireAt: expireSec * 1000 });
      }
    } else if (opcode === 0x00) {
      const { value: key, bytesRead: keyBytes } = readString(buffer, offset);
      offset += keyBytes;
      const { value, bytesRead: valueBytes } = readString(buffer, offset);
      offset += valueBytes;
      
      keys.set(key, { value, expireAt: null });
    }
  }
  return { keys };
}
export { parseRDB };

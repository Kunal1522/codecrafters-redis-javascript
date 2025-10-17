function expiry_checker(command,rkvp) {
  if (command[8].toLowerCase() == "px") {
    const expiry = command[10];
    console.log(expiry);
    setTimeout(() => {
      rkvp.set(command[4], `ille_pille_kille`);
    }, expiry);
  } else if (command[8].toLowerCase() == "ex") {
    const expiry = command[10];
    setTimeout(() => {
      rkvp.set(command[4], `ille_pille_kille`);
    }, expiry * 1000);
  }
}

function generateStreamId(rawId) {
  if (!rawId) return null;
  if (rawId.includes("-") && !rawId.endsWith("-*")) {
    return rawId;
  }
  let timestamp, sequence;
  if (rawId.endsWith("-*")) {
    timestamp = rawId.split("-")[0];
  } else if (rawId === "*") {
    timestamp = Date.now();
  }
  const prevSeq = streamSequenceMap.get(timestamp) ?? -1;

  sequence = prevSeq + 1;
  if (timestamp == 0 && sequence == 0) sequence++;
  streamSequenceMap.set(timestamp, sequence);

  const fullId = `${timestamp}-${sequence}`;
  return fullId;
}

export {expiry_checker,generateStreamId};

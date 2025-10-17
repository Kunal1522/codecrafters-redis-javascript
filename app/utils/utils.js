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


export {expiry_checker};

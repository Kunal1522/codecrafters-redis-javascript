import { subchannel } from "../state/store.js";

function addsubscriber(channel, client) {
  if (!subchannel.has(channel)) {
    subchannel.set(channel, []);
  }
  subchannel.get(channel).push(client);
}

function removesubscriber(channel, client) {
  if (!subchannel.has(channel)) {
    return;
  }
  const clients = subchannel.get(channel);
  const index = clients.indexOf(client);
  if (index !== -1) {
    clients.splice(index, 1);
  }
  if (clients.length === 0) {
    subchannel.delete(channel);
  }
}

function publisher(channel, msg) {
  const clients = subchannel.get(channel);
  if (!clients || clients.length === 0) {
    return 0;
  }
  
  for (const client of clients) {
    client.write(
      `*3\r\n$7\r\nmessage\r\n$${channel.length}\r\n${channel}\r\n$${msg.length}\r\n${msg}\r\n`
    );
  }
  
  return clients.length;
}

function subscribe_handler(command, connection, subscribedChannels, subscriber_mode) {
  subscriber_mode.active = true;
  const channel = command[1];
  addsubscriber(channel, connection);
  subscribedChannels.add(channel);
  const channel_len = subscribedChannels.size;
  const res = `*3\r\n$9\r\nsubscribe\r\n$${channel.length}\r\n${channel}\r\n:${channel_len}\r\n`;
  connection.write(res);
}

function unsubscribe_handler(command, connection, subscribedChannels, subscriber_mode) {
  const channel = command[1];
  removesubscriber(channel, connection);
  subscribedChannels.delete(channel);
  const channel_len = subscribedChannels.size;
  const res = `*3\r\n$11\r\nunsubscribe\r\n$${channel.length}\r\n${channel}\r\n:${channel_len}\r\n`;
  connection.write(res);
  
  if (subscribedChannels.size === 0) {
    subscriber_mode.active = false;
  }
}

function publish_handler(command, connection) {
  const channel = command[1];
  const message = command[2];
  const numSubscribers = publisher(channel, message);
  connection.write(`:${numSubscribers}\r\n`);
}

export { subscribe_handler, unsubscribe_handler, publish_handler };

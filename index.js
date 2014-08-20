var Through = require('pull-core').Through;

/**
  # pull-level-batch

  Gather upstream items with a valid `key` and `value` into optimal batches
  for writing with leveldb batch operations.

**/
module.exports = Through(function(read, maxSize, encoding) {
  var buffered = [];
  var currentSize = 0;

  // set default max size to match default leveldb write buffer size
  maxSize = maxSize || (4 * 1024 * 1024);

  return function(abort, cb) {
    function next(end, data) {
      var payload;
      var itemSize;

      if (end) {
        if (buffered.length > 0) {
          return cb(null, buffered.splice(0));
        }

        return cb(end, data);
      }

      // calculate the size of the item
      itemSize = Buffer.byteLength(data.key + data.value, encoding);

      // if this item will push us over, extract a payload
      if (currentSize + itemSize >= maxSize) {
        payload = buffered.splice(0);
        currentSize = 0;
      }

      // add the new item to the buffer
      buffered.push(data);
      currentSize += itemSize;

      // if we have a payload, then trigger the callback
      if (Array.isArray(payload) && payload.length > 0) {
        return cb(null, payload);
      }

      read(null, next);
    }

    if (abort) {
      return cb(abort);
    }

    read(abort, next);
  };
});

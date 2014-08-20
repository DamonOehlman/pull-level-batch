var looper = require('looper');
var Through = require('pull-core').Through;

/**
  # pull-level-batch

  Gather upstream items with a valid `key` and `value` into optimal batches
  for writing with leveldb batch operations.

  ## Usage

  See the tests.

**/
module.exports = function(maxSize, encoding) {
  var buffered = [];
  var currentSize = 0;
  var output = [];
  var ended = null;

  // set default max size to match default leveldb write buffer size
  maxSize = maxSize || (4 * 1024 * 1024);

  return function(read) {
    return function(abort, cb) {
      if (ended) {
        return cb(ended);
      }

      looper(function(next) {
        read(abort, function(end, data) {
          var itemSize;

          ended = ended || end;
          if (ended) {
            if (buffered.length || output.length) {
              return cb(null, buffered.splice(0).concat(output.splice(0)));
            }

            return cb(ended);
          }

          // calculate the size of the item
          itemSize = Buffer.byteLength(data.key + data.value, encoding);

          // if this item will push us over, extract a payload
          if (currentSize + itemSize > maxSize) {
            output = buffered.splice(0);
            currentSize = 0;
          }

          // add the new item to the buffer
          buffered.push(data);
          currentSize += itemSize;

          if (output.length) {
            return cb(null, output.splice(0));
          }

          next();
        });
      });
    };
  };
};

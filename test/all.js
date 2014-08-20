var test = require('tape');
var pull = require('pull-stream');
var batch = require('..');
var data = require('./data/simple');

test('a small max size ensures items are emitted one at a time', function(t) {
  t.plan(data.length);
  pull(
    pull.values(data),
    batch(1),
    pull.drain(function(batch) {
      t.equal(batch.length, 1);
    })
  );
});

test('we should be able to collect multiple items with a slightly larger batch size', function(t) {
  t.plan(data.length >> 1);
  pull(
    pull.values(data),
    batch(20),
    pull.drain(function(batch) {
      t.equal(batch.length, 2);
    })
  );
});

test('if the batch size is large enough all items should come through in a single hit', function(t) {
  t.plan(1);
  pull(
    pull.values(data),
    batch(7 * data.length),
    pull.drain(function(batch) {
      t.equal(batch.length, data.length);
    })
  );
});

var hat = require('hat')
var ldjson = require('ldjson-stream')
var duplexify = require('duplexify')
var events = require('events')
var util = require('util')

var Mortable = function() {
  if (!(this instanceof Mortable)) return new Mortable()
  events.EventEmitter.call(this)

  this.id = hat(64)

  this._head = 0
  this._peers = []

  this._handshake = {}
  this._changes = []
  this._table = {}
  this._owns = {}

  this._reads = []
}

util.inherits(Mortable, events.EventEmitter)

Mortable.prototype.createWriteStream = function() {
  var self = this
  return ldjson.parse()
    .on('data', function(change) {
      self._update(change)
    })
    .on('finish', function() {
      self.emit('sync')
    })
}

Mortable.prototype.createReadStream = function() {
  var serialize = ldjson.serialize()
  var self = this

  serialize.destroy = function() {
    var i = self._reads.indexOf(serialize)
    if (i === -1) return
    self._reads.splice(i, 1)
    serialize.emit('close')
  }

  for (var i = 0; i < this._changes.length; i++) {
    if (this._changes[i].id !== this.id) serialize.write(this._changes[i])
  }
  this._reads.push(serialize)

  return serialize
}

Mortable.prototype.owns = function(key, value) {
  return this._owns[key+'@'+value]
}

Mortable.prototype.push = function(key, value) {
  var id = key+'@'+value
  if (this._owns[id]) return
  this._owns[id] = true
  this._change('push', key, value)
}

Mortable.prototype.pull = function(key, value) {
  var id = key+'@'+value
  if (!this._owns[id]) return
  delete this._owns[id]
  this._change('pull', key, value)
}

Mortable.prototype.keys = function() {
  return Object.keys(this._table)
}

Mortable.prototype.has = function(key, value) {
  return !!this._table[key] && (arguments.length === 1 || this._table[key].indexOf(value) > -1)
}

Mortable.prototype.list = function(key) {
  return this._table[key]
}

Mortable.prototype._change = function(type, key, value) {
  this._update({type:type, id:this.id, change:++this._head, key:key, value:value})
}

Mortable.prototype._update = function(change) {
  this._handshake[change.id] = change.change
  this._changes.push(change)

  var t = this._table[change.key]

  if (change.type === 'push') {
    if (!t) t = this._table[change.key] = []
    t.push(change.value)
    this.emit('push', change.key, change.value)
  }
  if (change.type === 'pull') {
    var i = t.indexOf(change.value)
    if (i > -1) t.splice(i, 1)
    if (!t.length) delete this._table[change.key]
    this.emit('pull', change.key, change.value)
  }

  this.emit('update', change.key, change.value)

  for (var i = 0; i < this._peers.length; i++) {
    var pair = this._peers[i]
    var handshake = pair[0]
    var peer = pair[1]

    if (handshake[change.id] >= change.change) continue
    handshake[change.id] = change.change
    peer.write(change)
  }

  if (change.id === this.id) return

  for (var i = 0; i < this._reads.length; i++) {
    this._reads[i].write(change)
  }
}

Mortable.prototype.createStream = function() {
  var serialize = ldjson.serialize()
  var parse = ldjson.parse()

  var dup = duplexify.obj(parse, serialize)
  var self = this

  parse.once('data', function(handshake) {
    var pair = [handshake, serialize]

    self._peers.push(pair)
    dup.once('close', function() {
      self._peers.splice(self._peers.indexOf(pair), 1)
    })

    for (var i = 0; i < self._changes.length; i++) {
      var change = self._changes[i]
      var since = handshake[change.id] || 0

      if (since >= change.change) continue
      handshake[change.id] = change.change
      serialize.write(change)
    }

    parse.on('data', function(change) {
      handshake[change.id] = change.change
      self._update(change)
    })
  })

  serialize.write(this._handshake)

  return dup
}

module.exports = Mortable
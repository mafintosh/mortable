var cuid = require('cuid')
var protobufs = require('protocol-buffers-stream')
var once = require('once')
var util = require('util')
var EventEmitter = require('events').EventEmitter
var fs = require('fs')

var createStream = protobufs(fs.readFileSync(require.resolve('./mortable.proto')))

var PUSH = 1
var PULL = 2
var HEARTBEAT = 3
var QUIT = 4

var toArray = function(map) {
  return Object.keys(map).map(function(key) {
    return map[key]
  })
}

var Peer = function(id) {
  this.id = id
  this.seq = 0
  this.updated = 0
  this.changes = []
  this.values = {}
}

Peer.prototype.push = function(key, value) {
  var vals = this.values[key]
  if (!vals) vals = this.values[key] = []
  if (vals.indexOf(value) === -1) vals.push(value)
}

Peer.prototype.pull = function(key, value) {
  var vals = this.values[key]
  var i = vals ? vals.indexOf(value) : -1
  if (i === -1) return
  if (vals.length === 1) delete this.values[key]
  else vals.splice(i, 1)
}

Peer.prototype.has = function(key) {
  return !!this.values[key]
}

Peer.prototype.list = function(key) {
  return key === undefined ? Object.keys(this.values) : this.values[key]
}

Peer.prototype.update = function(change, table) {
  if (change.seq < this.seq) return false

  this.seq = change.seq
  this.updated = change.timestamp

  switch (change.op) {
    case PUSH:
    this.push(change.key, change.value)
    break

    case PULL:
    this.pull(change.key, change.value)
    break

    case QUIT:
    this.values = {}
    this.updated = 0
    this.changes = []
    break

    case HEARTBEAT:
    break

    default:
    return false
  }

  // trim heartbeats from the log
  if (this.changes.length && this.changes[this.changes.length-1].op === HEARTBEAT) this.changes.pop()
  this.changes.push(change)

  return true
}

var Mortable = function(opts) {
  if (!(this instanceof Mortable)) return new Mortable(opts)
  if (!opts) opts = {}

  EventEmitter.call(this)

  this.ttl = opts.ttl || 10000
  this.id = opts.id || cuid()

  var self = this
  var tick = function() {
    self.heartbeat()
  }

  this._peers = {}
  this._local = this._peers[this.id] = new Peer(this.id)
  this._heartbeat = setInterval(tick, (this.ttl / 2) | 0)
  this._streams = []

  if (this._heartbeat.unref) this._heartbeat.unref()
}

util.inherits(Mortable, EventEmitter)

Mortable.prototype.has = function(key) {
  var ids = Object.keys(this._peers)

  for (var i = 0; i < ids.length; i++) {
    if (this._peers[ids[i]].has(key)) return true
  }

  return false
}

Mortable.prototype.list = function(key) {
  var set = key === undefined ? {} : null
  var ids = Object.keys(this._peers)
  var now = Date.now()

  for (var i = 0; i < ids.length; i++) {
    var p = this._peers[ids[i]]
    if (p !== this._local && p.updated + this.ttl < now) continue

    var list = p.list(key)

    if (!list) continue
    if (!set) set = {}

    for (var j = 0; j < list.length; j++) set[list[j]] = true
  }

  return set && Object.keys(set)
}

Mortable.prototype.destroy = function() {
  clearInterval(this._heartbeat)

  this._change(QUIT, null, null)

  for (var i = 0; i < this._streams.length; i++) {
    this._streams[i].finalize()
  }
}

Mortable.prototype.createWriteStream = function() {
  var s = createStream()
  var self = this

  s.on('bulk', function(bulk) {
    self._applyAll(s, bulk.changes)
  })

  s.on('change', function(change) {
    self._apply(s, change)
  })

  return s
}

Mortable.prototype.createReadStream = function() {
  var s = createStream()
  var self = this
  var peers = toArray(this._peers)

  for (var i = 0; i < peers.length; i++) {
    var p = peers[i]
    for (var j = 0; j < p.changes.length; j++) s.change(p.changes[j])
  }

  return this._addStream(s)
}

Mortable.prototype.push = function(key, value) {
  this._change(PUSH, key, value)
}

Mortable.prototype.pull = function(key, value) {
  this._change(PULL, key, value)
}

Mortable.prototype.heartbeat = function() {
  this._change(HEARTBEAT, null, null)
}

Mortable.prototype._change = function(op, key, value) {
  this._apply(null, {
    op: op,
    seq: this._local.seq+1,
    timestamp: Date.now(),
    peer: this.id,
    key: key,
    value: value
  })
}

Mortable.prototype._update = function(from, change) {
  var peer = this._peers[change.peer] || (this._peers[change.peer] = new Peer(change.peer))
  if (!peer.update(change, this)) return false

  for (var i = 0; i < this._streams.length; i++) {
    if (this._streams[i] !== from) this._streams[i].change(change)
  }

  return true
}

Mortable.prototype._addStream = function(stream, ondone) {
  var self = this

  var cleanup = once(function() {
    var i = self._streams.indexOf(stream)
    if (i > -1) self._streams.splice(i, 1)
    if (ondone) ondone()
  })

  stream.on('close', cleanup).on('end', cleanup).on('finish', cleanup)
  this._streams.push(stream)

  return stream
}

Mortable.prototype._apply = function(from, change) {
  if (!this._update(from, change)) return
  if (change.key) this.emit('update', change.key)
}

Mortable.prototype._applyAll = function(from, changes) {
  var set = {}

  for (var i = 0; i < changes.length; i++) {
    if (!this._update(from, changes[i])) continue
    if (changes[i].key) set[changes[i].key] = true
  }

  var keys = Object.keys(set)
  for (var i = 0; i < keys.length; i++) this.emit('update', keys[i])
}

var addSeq = function(result, peer) {
  result[peer.id] = peer.seq
  return result
}

Mortable.prototype.createStream = function() {
  var self = this
  var s = createStream()

  s.sync({
    id: this.id,
    peers: toArray(this._peers)
  })

  s.once('sync', function(sync) {
    var seqs = sync.peers.reduce(addSeq, {})
    var peers = toArray(self._peers)

    for (var i = 0; i < peers.length; i++) {
      var p = peers[i]
      var seq = seqs[p.id] || 0
      var changes = []

      for (var j = 0; j < p.changes.length; j++) {
        if (p.changes[j].seq > seq) changes.push(p.changes[j])
      }

      s.bulk({changes:changes})
    }

    s.on('bulk', function(bulk) {
      self._applyAll(s, bulk.changes)
      onalive()
    })

    s.on('change', function(change) {
      self._apply(s, change)
      onalive()
    })

    var alive = true

    var tick = function() {
      if (!alive) s.destroy()
      else alive = false
    }

    var onalive = function() {
      alive = true
    }

    var ticker = setInterval(tick, (self.ttl / 2) | 0)
    if (ticker.unref) ticker.unref()

    self._addStream(s, function() {
      clearInterval(ticker)
    })
  })

  return s
}

module.exports = Mortable
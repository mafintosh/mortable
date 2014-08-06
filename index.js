var protobufs = require('protocol-buffers-stream')
var once = require('once')
var fs = require('fs')
var cuid = require('cuid')
var events = require('events')
var util = require('util')

var createStream = protobufs(fs.readFileSync(require.resolve('./schema.proto')))

// op enum

var PUSH = 1
var PULL = 2
var HEARTBEAT = 3
var FIN = 4

var toArray = function(map) {
  return Object.keys(map).map(function(key) {
    return map[key]
  })
}

var Peer = function(id) {
  this.id = id
  this.seq = 0
  this.heartbeat = 0
  this.changes = []

  this._sets = {}
}

Peer.prototype.push = function(key, val) {
  var set = this._sets[key]
  if (!set) set = this._sets[key] = []
  if (set.indexOf(key) === -1) set.push(val)
}

Peer.prototype.pull = function(key, val) {
  var set = this._sets[key] || []
  var i = set.indexOf(val)
  if (i === -1) return

  if (set.length === 1) delete this._sets[key]
  else set.splice(i, 1)
}

Peer.prototype.list = function(key) {
  return key === undefined ? Object.keys(this._sets) : (this._sets[key] || null)
}

var Mortable = function(id) {
  if (!(this instanceof Mortable)) return new Mortable(id)
  events.EventEmitter.call(this)

  this.id = id || cuid()
  this.peers = {}
  this.seq = 0
  this.heartbeat = 0
  this.streams = []
  this.changes = []

  var self = this
  var update = function() {
    self.heartbeat++
  }

  this.interval = setInterval(update, 10000)
  if (this.interval.unref) this.interval.unref()
}

util.inherits(Mortable, events.EventEmitter)

Mortable.prototype.destroy = function() {
  clearInterval(this.interval)
  this._change(FIN, null, null)
  for (var i = 0; i < this.streams.length; i++) {
    this.streams[i].finalize()
  }
}

Mortable.prototype.push = function(key, val) {
  this._change(PUSH, key, val)
}

Mortable.prototype.pull = function(key, val) {
  this._change(PULL, key, val)
}

Mortable.prototype.list = function(key) {
  var list = Object.keys(this.peers)
  var result = {}
  for (var i = 0; i < list.length; i++) {
    var p = this.peers[list[i]]
    if (!p || p.heartbeat <= this.heartbeat) continue
    var l = p.list(key)
    if (!l) continue
    for (var j = 0; j < l.length; j++) result[l[j]] = true
  }
  return Object.keys(result)
}

Mortable.prototype._change = function(op, key, val) {
  this._update({op:op, seq:++this.seq, from:this.id, key:key, value:val}, null)
}

Mortable.prototype._update = function(change, stream) {
  var p = this.peers[change.from]
  if (p === null) return // was permanently deleted
  if (!p) p = this.peers[change.from] = new Peer(change.from)

  if (p.seq >= change.seq) return
  p.seq = change.seq

  switch (change.op) {
    case PUSH:
    p.push(change.key, change.value)
    this.emit('push', change.key, change.value, change.from)
    this.emit('update', change.key, change.value)
    this.changes.push(change)
    break

    case PULL:
    p.pull(change.key, change.value)
    this.emit('pull', change.key, change.value, change.from)
    this.emit('update', change.key, change.value)
    this.changes.push(change)
    break

    case HEARTBEAT:
    p.heartbeat = this.heartbeat+2
    break

    case FIN:
    this.peers[p.id] = null
    this.changes = this.changes.filter(function(change) {
      return change.from !== p.id
    })
    break

    default:
    return
  }

  for (var i = 0; i < this.streams.length; i++) {
    if (this.streams[i] !== stream) this.streams[i].change(change)
  }
}

Mortable.prototype.createStream = function() {
  var s = createStream()
  var self = this

  var heartbeat = function() {
    self._change(HEARTBEAT, null, null)
  }

  s.once('handshake', function(handshake) {
    var interval = setInterval(heartbeat, 5000)
    if (interval.unref) interval.unref()

    var cleanup = once(function() {
      clearInterval(interval)
      self.streams.splice(self.streams.indexOf(s), 1)
    })

    s.on('close', cleanup).on('finish', cleanup).on('end', cleanup)

    s.on('heartbeat', function(heartbeat) {
      var p = self.peers[heartbeat.from]
      if (p && heartbeat.seq >= p.seq) p.heartbeat = self.heartbeat+1
    })

    s.on('change', function(change) {
      self._update(change, s)
    })

    // index seqs
    var seqs = handshake.peers.reduce(function(result, peer) {
      result[peer.id] = peer.seq
      return result
    }, {})

    // send local changes to the remote
    for (var i = 0; i < self.changes.length; i++) {
      var change = self.changes[i]
      var seq = seqs[change.from] || 0
      if (change.seq > seq) s.change(change)
    }

    self.streams.push(s)
    heartbeat()
  })

  s.handshake({
    from: this.id,
    peers: toArray(this.peers)
  })

  return s
}

module.exports = Mortable

if (require.main !== module) return

var m1 = Mortable('peer1')
var m2 = Mortable('peer2')

m1.push('hi', 'world')
m1.push('hi', 'der der')

var s1 = m1.createStream()
var s2 = m2.createStream()

s1.pipe(s2).pipe(s1)

setImmediate(function() {
  console.log(m2.list('hi'))
  m1.destroy()
  console.log(m2.list('hi'))
}, 100)

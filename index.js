var cuid = require('cuid')
var protobufs = require('protocol-buffers-stream')
var once = require('once')
var util = require('util')
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
  this.heartbeat = 0
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

  // do not record heartbeat messages
  if (change.op === HEARTBEAT) {
    this.heartbeat = table._tick+1
    return true
  }

  switch (change.op) {
    case PUSH:
    this.push(change.key, change.value)
    break

    case PULL:
    this.pull(change.key, change.value)
    break

    case QUIT:
    this.heartbeat = 0
    this.changes = []
    break

    default:
    return false
  }

  this.changes.push(change)

  return true
}

var Mortable = function(opts) {
  if (!(this instanceof Mortable)) return new Mortable(opts)
  if (!opts) opts = {}

  this.ttl = opts.ttl || 10000
  this.id = opts.id || cuid()

  var self = this
  var tick = function() {
    self._tick++
    self.heartbeat()
  }

  this._tick = 1
  this._peers = {}
  this._local = this._peers[this.id] = new Peer(this.id)
  this._interval = setInterval(tick, (this.ttl / 2) | 0)
  this._streams = []

  if (this._interval.unref) this._interval.unref()
}

Mortable.prototype.has = function(key) {
  var ids = Object.keys(this._peers)

  for (var i = 0; i < ids.length; i++) {
    if (this._peers[ids[i]].has(key)) return true
  }

  return false
}

Mortable.prototype.list = function(key) {
  var set = arguments.length === 0 ? {} : null
  var ids = Object.keys(this._peers)

  for (var i = 0; i < ids.length; i++) {
    var p = this._peers[ids[i]]
    if (p !== this._local && p.heartbeat < this._tick) continue

    var list = p.list(key)

    if (!list) continue
    if (!set) set = {}

    for (var j = 0; j < list.length; j++) set[list[j]] = true
  }

  return set && Object.keys(set)
}

Mortable.prototype.destroy = function() {
  clearInterval(this._interval)

  this._update(null, {
    op: QUIT,
    peer: this.id,
    seq: this._local.seq+1
  })

  for (var i = 0; i < this._streams.length; i++) {
    this._streams[i].finalize()
  }
}

Mortable.prototype.push = function(key, value) {
  this._update(null, {
    op: PUSH,
    peer: this.id,
    seq: this._local.seq+1,
    key: key,
    value: value
  })
}

Mortable.prototype.pull = function(key, value) {
  this._update(null, {
    op: PULL,
    peer: this.id,
    seq: this._local.seq+1,
    key: key,
    value: value
  })
}

Mortable.prototype.heartbeat = function() {
  this._update(null, {
    op: HEARTBEAT,
    peer: this.id,
    seq: this._local.seq+1
  })
}

Mortable.prototype._update = function(from, change) {
  var peer = this._peers[change.peer] || (this._peers[change.peer] = new Peer(change.peer))
  if (!peer.update(change, this)) return

  for (var i = 0; i < this._streams.length; i++) {
    if (this._streams[i] !== from) this._streams[i].change(change)
  }
}

var addSeq = function(result, peer) {
  result[peer.id] = peer.seq
  return result
}

Mortable.prototype.createStream = function() {
  var self = this
  var s = createStream()

  s.sync({
    peers: toArray(this._peers)
  })

  s.once('sync', function(sync) {
    var seqs = sync.peers.reduce(addSeq, {})
    var peers = toArray(self._peers)

    var cleanup = once(function() {
      var i = self._streams.indexOf(s)
      if (i > -1) self._streams.splice(i, 1)
    })

    for (var i = 0; i < peers.length; i++) {
      var p = peers[i]
      var seq = seqs[p.id] || 0

      for (var j = 0; j < p.changes.length; j++) {
        if (p.changes[j].seq > seq) s.change(p.changes[j])
      }
    }

    s.on('finish', cleanup).on('end', cleanup).on('close', cleanup)

    s.on('change', function(change) {
      self._update(s, change)
    })

    self._streams.push(s)
    self.heartbeat() // we send a heartbeat to indicate that we are alive
  })

  return s
}

module.exports = Mortable

if (require.main !== module) return

var m = Mortable()
var s = Mortable()

m.push('hello', 'world')
m.push('hello', 'verden')

var ms = m.createStream()

ms.pipe(s.createStream()).pipe(ms)

setImmediate(function() {
  console.log(s.list())
  console.log(s.list('hello'))
})

//console.log(m.has('hello1'), m.list('hello1'))
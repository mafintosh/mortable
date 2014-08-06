#!/usr/bin/env node

var mortable = require('../')
var net = require('net')
var network = require('network-address')
var pump = require('pump')
var hprotocol = require('hprotocol')

var host = process.argv[2] && process.argv[2].split(':')[0]
var port = process.argv[2] && parseInt(process.argv[2].split(':')[1])
var table = mortable()

if (!host || !port) return console.error('Usage: ./client.js host:port')

var connect = function(port, host) {
  var socket = net.connect(port, host)

  socket.on('error', function() {
    socket.destroy()
  })

  socket.on('connect', function() {
    console.log('(connected to %s:%d)', host, port)
  })

  socket.on('close', function() {
    console.log('(disconnected from %s:%d)', host, port)
    setTimeout(function() {
      connect(port, host)
    }, 5000)
  })

  pump(socket, table.createStream(), socket)
}

connect(port, host)

table.on('update', function(key) {
  console.log('(%s was updated)', key)
})

var protocol = hprotocol()
  .use('push key value')
  .use('pull key value')
  .use('list key > values...')
  .use('connect addr')
  .use('changes')

var client = protocol()

// table.createReadStream().pipe(require('fs').createWriteStream('log'))

client.on('changes', function() {
  console.log(table.changes)
})

client.on('connect', function(addr) {
  connect(addr.split(':')[1], addr.split(':')[0])
})

client.on('push', function(key, value) {
  table.push(key, value)
})

client.on('pull', function(key, value) {
  table.pull(key, value)
})

client.on('list', function(key, cb) {
  cb(null, table.list(key))
})

process.stdin.pipe(client.stream).pipe(process.stdout)
process.stdout.write(protocol.specification)


process.on('SIGINT', function() {
  table.destroy()
  setTimeout(process.exit, 100)
})

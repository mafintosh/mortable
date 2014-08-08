#!/usr/bin/env node

var mortable = require('../')
var net = require('net')
var network = require('network-address')
var pump = require('pump')
var hprotocol = require('hprotocol')

var listen = process.argv[2]
var host = process.argv[3] && process.argv[3].split(':')[0]
var port = process.argv[3] && parseInt(process.argv[3].split(':')[1])
var table = mortable()

if (host === 'localhost') host = network()

var connected = {}
var connect = function(port, host) {
  if (connected[host+':'+port]) return
  connected[host+':'+port] = true

  var socket = net.connect(port, host)
  var alive = false

  socket.on('error', function() {
    socket.destroy()
  })

  socket.on('connect', function() {
    alive = true
    console.log('(connected to %s:%d)', host, port)
  })

  socket.on('close', function() {
    if (alive) console.log('(disconnected from %s:%d)', host, port)
    setTimeout(function() {
      connect(port, host)
    }, 5000)
  })

  pump(socket, table.createStream(), socket)
}

var me

var server = net.createServer(function(socket) {
  pump(socket, table.createStream(), socket)
})

server.listen(parseInt(listen), function() {
  console.log('hub is listening on port %s:%d', network(), server.address().port)
  table.push('hubs', me = network()+':'+server.address().port)
})

table.on('change', function(key) {
  if (key !== 'hubs') return
  var hubs = table.list('hubs') || []
  hubs.forEach(function(addr) {
    connect(addr.split(':')[1], addr.split(':')[0])
  })
})

if (port) connect(port, host)

process.on('SIGINT', function() {
  table.destroy()
  setTimeout(process.exit, 100)
})

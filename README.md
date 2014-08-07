# mortable

In memory eventual consistant replicatable p2p data structure

```
npm install mortable
```

[![build status](http://img.shields.io/travis/mafintosh/mortable.svg?style=flat)](http://travis-ci.org/mafintosh/mortable)

Mortable is for sharing data that is only valid as long as your process is alive and small
enough to fit in memory (think hostnames+ports for a distributed service registry)

Data will be replicated to other nodes using protobuf messages and the scuttlebutt protocol

## Usage

``` js
var mortable = require('mortable')
var table = mortable()

table.push('hello', 'world')     // push world to the hello data set
console.log(table.list('hello')) // prints ['world']

var table2 = mortable()

table2.push('hello', 'mundo')

var stream = table.createStream()
stream.pipe(table2).pipe(stream) // start replicating between the two tables

setImmediate(function() {
  console.log(table2.list('hello')) // will print ['world', 'mundo']
})
```

To replicate the data structure to other nodes use `.createStream()` and pipe it to a network stream

```
var stream = table.createStream()

// will start replicating this data structure to other nodes
streamToAnotherPeer.pipe(stream).pipe(streamToAnotherPeer)
```

If you want to destoy your table call `table.destroy()` which will remove all local values from the replicas.
If you do not call destroy and exits your process the values will be removed after a timeout (~10s)

## Credits

Credits to [dominictarr](github.com/dominictarr/) for making the excellent scuttlebutt module that inspired this

## License

MIT
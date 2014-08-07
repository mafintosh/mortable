var tape = require('tape')
var mortable = require('./')

tape('one instance', function(t) {
  var m = mortable()

  m.push('hello', 'world')
  t.same(m.list(), ['hello'])
  t.same(m.list('hello'), ['world'])
  m.pull('hello', 'world')
  t.same(m.list(), [])
  t.same(m.list('hello'), null)
  t.end()
})

tape('two instances', function(t) {
  var m1 = mortable()
  var m2 = mortable()

  m1.push('hello', 'world')

  var s1 = m1.createStream()
  var s2 = m2.createStream()

  s1.pipe(s2).pipe(s1)

  setImmediate(function() {
    t.same(m2.list('hello'), ['world'])
    t.end()
  })
})

tape('two instances + concurrent updates', function(t) {
  var m1 = mortable()
  var m2 = mortable()

  m1.push('hello', 'world')
  m2.push('hello', 'welt')

  var s1 = m1.createStream()
  var s2 = m2.createStream()

  s1.pipe(s2).pipe(s1)

  setImmediate(function() {
    t.same(m2.list('hello').sort(), ['welt', 'world'])
    t.end()
  })
})

tape('two instances + no timeout', function(t) {
  var m1 = mortable({ttl:100})
  var m2 = mortable()

  m1.push('hello', 'world')
  m2.push('hello', 'welt')

  var s1 = m1.createStream()
  var s2 = m2.createStream()

  s1.pipe(s2).pipe(s1)

  setImmediate(function() {
    t.same(m2.list('hello').sort(), ['welt', 'world'])
    setTimeout(function() {
      t.same(m2.list('hello').sort(), ['welt', 'world'])
      t.end()
    }, 500)
  })
})

tape('two instances + timeout', function(t) {
  var m1 = mortable({ttl:100})
  var m2 = mortable()

  m1.push('hello', 'world')
  m2.push('hello', 'welt')

  var s1 = m1.createStream()
  var s2 = m2.createStream()

  s1.pipe(s2).pipe(s1)

  setImmediate(function() {
    t.same(m2.list('hello').sort(), ['welt', 'world'])
    s2.end()
    setTimeout(function() {
      t.same(m1.list('hello'), ['world'])
      t.end()
    }, 500)
  })
})

tape('two instances + destroy', function(t) {
  var m1 = mortable()
  var m2 = mortable()

  m1.push('hello', 'world')
  m2.push('hello', 'welt')

  var s1 = m1.createStream()
  var s2 = m2.createStream()

  s1.pipe(s2).pipe(s1)

  setImmediate(function() {
    t.same(m2.list('hello').sort(), ['welt', 'world'])
    m2.destroy()
    setImmediate(function() {
      t.same(m1.list('hello'), ['world'])
      t.end()
    })
  })
})
const p = require('path')
const ram = require('random-access-memory')
const raf = require('random-access-file')
const bitwebEncoding = require('@web4/encoding')
const bitwebCrypto = require('@web4/bitweb-crypto')
const test = require('tape')

const Chainstore = require('..')
const {
  runAll,
  validateChain,
  cleanup
} = require('./helpers')

test('ram-based chainstore, different get options', async t => {
  const store1 = await create(ram)
  const chain1 = store1.default()
  var chain2, chain3, chain4, chain5, chain6

  await runAll([
    cb => chain1.ready(cb),
    cb => chain1.append('hello', cb),
    cb => {
      // Buffer arg
      chain2 = store1.get(chain1.key)
      return chain2.ready(cb)
    },
    cb => {
      // Object arg
      chain3 = store1.get({ key: chain1.key })
      return chain3.ready(cb)
    },
    cb => {
      // Discovery key option
      chain4 = store1.get({ discoveryKey: chain1.discoveryKey })
      return chain4.ready(cb)
    },
    cb => {
      // String option
      chain5 = store1.get({ key: bitwebEncoding.encode(chain1.key) })
      return chain5.ready(cb)
    },
    cb => {
      // Custom keypair option
      chain6 = store1.get({ keyPair: { secretKey: chain1.secretKey, publicKey: chain1.key } })
      return chain6.ready(cb)
    }
  ])

  t.same(chain1, chain2)
  t.same(chain1, chain3)
  t.same(chain1, chain4)
  t.same(chain1, chain5)
  t.same(chain1, chain6)
  t.end()
})

test('ram-based chainstore, simple replication', async t => {
  const store1 = await create(ram)
  const store2 = await create(ram)
  const chain1 = store1.default()
  const chain2 = store1.get()
  var chain3 = null
  var chain4 = null

  await runAll([
    cb => chain1.ready(cb),
    cb => chain2.ready(cb),
    cb => {
      chain3 = store2.default(chain1.key)
      return chain3.ready(cb)
    },
    cb => {
      chain4 = store2.get({ key: chain2.key })
      return chain4.ready(cb)
    },
    cb => chain1.append('hello', cb),
    cb => chain1.append('world', cb),
    cb => chain2.append('cat', cb),
    cb => chain2.append('dog', cb),
    cb => {
      const stream = store1.replicate(true)
      stream.pipe(store2.replicate(false)).pipe(stream)
      stream.on('end', cb)
    }
  ])

  await validateChain(t, chain3, [Buffer.from('hello'), Buffer.from('world')])
  await validateChain(t, chain4, [Buffer.from('cat'), Buffer.from('dog')])

  t.end()
})

test('ram-based chainstore, replicating with different default keys', async t => {
  const store1 = await create(ram)
  const store2 = await create(ram)
  const chain1 = store1.default()
  const chain2 = store1.get()
  var chain3 = null
  var chain4 = null

  await runAll([
    cb => chain1.ready(cb),
    cb => chain2.ready(cb),
    cb => {
      chain3 = store2.default()
      return chain3.ready(cb)
    },
    cb => {
      chain4 = store2.get({ key: chain1.key })
      return chain4.ready(cb)
    },
    cb => chain1.append('cat', cb),
    cb => chain1.append('dog', cb),
    cb => {
      const stream = store1.replicate(true)
      stream.pipe(store2.replicate(false)).pipe(stream)
      stream.on('end', cb)
    }
  ])

  await validateChain(t, chain4, [Buffer.from('cat'), Buffer.from('dog')])
  t.end()
})

test('ram-based chainstore, sparse replication', async t => {
  const store1 = await create(ram, { sparse: true })
  const store2 = await create(ram, { sparse: true })
  const chain1 = store1.default()
  const chain2 = store1.get()
  var chain3 = null
  var chain4 = null

  await runAll([
    cb => chain1.ready(cb),
    cb => chain2.ready(cb),
    cb => {
      t.same(chain2.sparse, true)
      t.same(chain1.sparse, true)
      return process.nextTick(cb, null)
    },
    cb => {
      chain3 = store2.default(chain1.key)
      return chain3.ready(cb)
    },
    cb => {
      chain4 = store2.get({ key: chain2.key })
      return chain4.ready(cb)
    },
    cb => {
      const stream = store1.replicate(true, { live: true })
      stream.pipe(store2.replicate(false, { live: true })).pipe(stream)
      return process.nextTick(cb, null)
    },
    cb => chain1.append('hello', cb),
    cb => chain1.append('world', cb),
    cb => chain2.append('cat', cb),
    cb => chain2.append('dog', cb),
    cb => {
      t.same(chain3.length, 0)
      t.same(chain4.length, 0)
      return process.nextTick(cb, null)
    }
  ])

  await validateChain(t, chain3, [Buffer.from('hello'), Buffer.from('world')])
  await validateChain(t, chain4, [Buffer.from('cat'), Buffer.from('dog')])
  t.end()
})

test('ram-based chainstore, sparse replication with different default keys', async t => {
  const store1 = await create(ram, { sparse: true })
  const store2 = await create(ram, { sparse: true })
  const chain1 = store1.default()
  var chain3 = null
  var chain4 = null

  await runAll([
    cb => chain1.ready(cb),
    cb => {
      chain3 = store2.default()
      return chain3.ready(cb)
    },
    cb => {
      const s1 = store1.replicate(true, { live: true })
      const s2 = store2.replicate(false, { live: true })
      s1.pipe(s2).pipe(s1)
      return process.nextTick(cb, null)
    },
    cb => chain1.append('cat', cb),
    cb => chain1.append('dog', cb),
    cb => {
      chain4 = store2.get({ key: chain1.key })
      return chain4.ready(cb)
    },
    cb => {
      t.same(chain4.length, 0)
      t.same(chain1.length, 2)
      return process.nextTick(cb, null)
    }
  ])

  await validateChain(t, chain4, [Buffer.from('cat'), Buffer.from('dog')])
  t.end()
})

test('raf-based chainstore, simple replication', async t => {
  const store1 = await create(path => raf(p.join('store1', path)))
  const store2 = await create(path => raf(p.join('store2', path)))
  const chain1 = store1.default()
  const chain2 = store1.get()
  var chain3 = null
  var chain4 = null

  await runAll([
    cb => chain1.ready(cb),
    cb => chain2.ready(cb),
    cb => {
      chain3 = store2.default({ key: chain1.key })
      return chain3.ready(cb)
    },
    cb => {
      chain4 = store2.get({ key: chain2.key })
      return chain4.ready(cb)
    },
    cb => chain1.append('hello', cb),
    cb => chain1.append('world', cb),
    cb => chain2.append('cat', cb),
    cb => chain2.append('dog', cb),
    cb => {
      setImmediate(() => {
        const stream = store1.replicate(true)
        stream.pipe(store2.replicate(false)).pipe(stream)
        stream.on('end', cb)
      })
    }
  ])

  await validateChain(t, chain3, [Buffer.from('hello'), Buffer.from('world')])
  await validateChain(t, chain4, [Buffer.from('cat'), Buffer.from('dog')])
  await cleanup(['store1', 'store2'])
  t.end()
})

test('raf-based chainstore, close and reopen', async t => {
  var store = await create('test-store')
  var firstChain = store.default()
  var reopenedChain = null

  await runAll([
    cb => firstChain.ready(cb),
    cb => firstChain.append('hello', cb),
    cb => store.close(cb),
    cb => {
      t.true(firstChain.closed)
      return process.nextTick(cb, null)
    },
    cb => {
      create('test-store').then(store => {
        reopenedChain = store.default()
        return reopenedChain.ready(cb)
      })
    }
  ])

  await validateChain(t, reopenedChain, [Buffer.from('hello')])
  await cleanup(['test-store'])
  t.end()
})

test('raf-based chainstore, close and reopen with keypair option', async t => {
  var store = await create('test-store')
  const keyPair = bitwebCrypto.keyPair()
  var firstChain = store.get({ keyPair })
  var reopenedChain = null

  await runAll([
    cb => firstChain.ready(cb),
    cb => firstChain.append('hello', cb),
    cb => store.close(cb),
    cb => {
      t.true(firstChain.closed)
      return process.nextTick(cb, null)
    },
    cb => {
      create('test-store').then(store => {
        reopenedChain = store.get({ key: keyPair.publicKey })
        return reopenedChain.ready(cb)
      })
    }
  ])

  await validateChain(t, reopenedChain, [Buffer.from('hello')])
  t.true(reopenedChain.writable)

  await cleanup(['test-store'])
  t.end()
})

test('live replication with an additional chain', async t => {
  const store1 = await create(ram)
  const store2 = await create(ram)

  const chain1 = store1.default()
  var chain2 = null
  var chain3 = null
  var chain4 = null

  await runAll([
    cb => chain1.ready(cb),
    cb => {
      chain3 = store2.default({ key: chain1.key })
      return chain3.ready(cb)
    },
    cb => {
      const stream = store1.replicate(true, { live: true })
      stream.pipe(store2.replicate(false, { live: true })).pipe(stream)
      return cb(null)
    },
    cb => {
      chain2 = store1.get()
      return chain2.ready(cb)
    },
    cb => {
      chain4 = store2.get(chain2.key)
      return chain4.ready(cb)
    },
    cb => chain2.append('hello', cb),
    cb => chain2.append('world', cb)
  ])

  await validateChain(t, chain4, [Buffer.from('hello'), Buffer.from('world')])
  t.end()
})

test('namespaced chainstores use separate default keys', async t => {
  const store1 = await create(ram)
  const store2 = store1.namespace('store2')
  const store3 = store1.namespace('store3')

  await store2.ready()
  await store3.ready()

  const feed1 = store2.default()
  const feed2 = store3.default()

  t.true(!feed1.key.equals(feed2.key))

  t.end()
})

test('namespaced chainstores will not increment reference multiple times', async t => {
  const store1 = await create(ram)
  const store2 = store1.namespace('store2')
  const store3 = store1.namespace('store3')

  await store2.ready()
  await store3.ready()

  const feed1 = store2.default()
  await feed1.ready()
  const feed3 = store3.get({ key: feed1.key })
  const feed4 = store3.get({ key: feed1.key })
  const feed5 = store3.get({ key: feed1.key })

  t.same(feed1, feed3)
  t.same(feed1, feed4)
  t.same(feed1, feed5)

  const entry = store1.cache.entry(feed1.discoveryKey.toString('hex'))
  t.same(entry.refs, 2)

  t.end()
})

test('namespaced chainstores can be nested', async t => {
  const store1 = await create(ram)
  const store2 = store1.namespace('store2')
  const store1a = store1.namespace('a')
  const store2a = store2.namespace('a')

  const feed1 = store1.default()
  const feed2 = store2.default()
  const feed1a = store1a.default()
  const feed2a = store2a.default()

  await feed1.ready()
  await feed2.ready()
  await feed1a.ready()
  await feed2a.ready()

  t.notEqual(feed1a.key, feed2a.key)

  t.end()
})

test('caching works correctly when reopening by discovery key', async t => {
  var store = await create('test-store')
  var firstChain = store.default()
  var discoveryKey = null
  var reopenedChain = null

  await runAll([
    cb => firstChain.ready(cb),
    cb => {
      discoveryKey = firstChain.discoveryKey
      return cb(null)
    },
    cb => firstChain.append('hello', cb),
    cb => store.close(cb),
    cb => {
      t.true(firstChain.closed)
      return process.nextTick(cb, null)
    },
    cb => {
      create('test-store').then(reopenedStore => {
        store = reopenedStore
        reopenedChain = store.get({ discoveryKey })
        return reopenedChain.ready(cb)
      })
    },
    cb => {
      const idx = discoveryKey.toString('hex')
      t.true(store.cache.has(idx))
      return cb(null)
    }
  ])

  await validateChain(t, reopenedChain, [Buffer.from('hello')])
  await cleanup(['test-store'])
  t.end()
})

test('can check if chains are loaded', async t => {
  const store = await create(ram)
  await store.ready()

  const feed1 = store.default()
  const feed2 = store.get()
  const badKey = bitwebCrypto.randomBytes(32)
  const badDiscoveryKey = bitwebCrypto.discoveryKey(badKey)

  t.true(store.isLoaded({ key: feed1.key }))
  t.true(store.isLoaded({ discoveryKey: feed1.discoveryKey }))
  t.true(store.isLoaded({ discoveryKey: feed2.discoveryKey }))
  t.false(store.isLoaded({ key: badKey }))
  t.false(store.isLoaded({ key: badDiscoveryKey }))

  t.end()
})

test('top-level chainstore replicates all opened chains', async t => {
  const store1 = await create(ram)
  const store2 = await create(ram)

  const chain1 = store1.default()
  const ns1 = store1.namespace()
  var chain2 = ns1.default()
  var chain3 = null

  await runAll([
    cb => chain1.ready(cb),
    cb => chain2.ready(cb),
    cb => {
      // Only replicate the top-level chainstore
      const stream = store1.replicate(true, { live: true })
      stream.pipe(store2.replicate(false, { live: true })).pipe(stream)
      return cb(null)
    },
    cb => {
      chain3 = store2.get(chain2.key)
      return chain3.ready(cb)
    },
    cb => chain2.append('hello', cb),
    cb => chain2.append('world', cb)
  ])

  await validateChain(t, chain3, [Buffer.from('hello'), Buffer.from('world')])
  t.end()
})
async function create (storage, opts) {
  const store = new Chainstore(storage, opts)
  await store.ready()
  return store
}

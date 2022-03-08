# chainstore

This module is the canonical implementation of the "chainstore" interface, which exposes a Unichain factory and a set of associated functions for managing generated Unichains.

A chainstore is designed to efficiently store and replicate multiple sets of interlinked Unichains, such as those used by [BitDrive](https://github.com/bitwebs/bitdrive) and [mountable-bittrie](https://github.com/bitwebs/bitrie), removing the responsibility of managing custom storage/replication code from these higher-level modules.

In order to do this, chainstore provides:
1. __Key derivation__ - all writable Unichain keys are derived from a single master key.
2. __Caching__ - Two separate caches are used for passively replicating chains (those requested by peers) and active chains (those requested by the owner of the chainstore).
3. __Storage bootstrapping__ - You can create a `default` Unichain that will be loaded when a key is not specified, which is useful when you don't want to reload a previously-created Unichain by key.
4. __Namespacing__ - If you want to create multiple compound data structures backed by a single chainstore, you can create namespaced chainstores such that each data structure's `default` feed is separate.

### Installation
`npm i @web4/chainstore --save`

### Usage
A chainstore instance can be constructed with a random-access-storage module, a function that returns a random-access-storage module given a path, or a string. If a string is specified, it will be assumed to be a path to a local storage directory:
```js
const Chainstore = require('@web4/chainstore')
const ram = require('random-access-memory')
const store = new Chainstore(ram)
await store.ready()
```

Unichains can be generated with both the `get` and `default` methods. If the first writable chain is created with `default`, it will be used for storage bootstrapping. We can always reload this bootstrapping chain off disk without your having to store its public key externally. Keys for other unichains should either be stored externally, or referenced from within the default chain:
```js
const chain1 = store1.default()
```
_Note: You do not have to create a default feed before creating additional ones unless you'd like to bootstrap your chainstore from disk the next time it's instantiated._

Additional unichains can be created by key, using the `get` method. In most scenarios, these additional keys can be extracted from the default (bootstrapping) chain. If that's not the case, keys will have to be stored externally:
```js
const chain2 = store1.get({ key: Buffer(...) })
```
All unichains are indexed by their discovery keys, so that they can be dynamically injected into replication streams when requested.

Two chainstores can be replicated with the `replicate` function, which accepts unichain's `replicate` options:
```js
const store1 = new Chainstore(ram)
const store2 = new Chainstore(ram)
await Promise.all([store1.ready(), store2.ready()]

const chain1 = store2.get()
const chain2 = store2.get({ key: chain1.key })
const stream = store1.replicate(true, { live: true })
stream.pipe(store2.replicate(false, { live: true })).pipe(stream) // This will replicate all common chains.
```

### API
#### `const store = chainstore(storage, [opts])`
Create a new chainstore instance. `storage` can be either a random-access-storage module, or a function that takes a path and returns a random-access-storage instance.

Opts is an optional object which can contain any Unichain constructor options, plus the following:
```js
{
  cacheSize: 1000 // The size of the LRU cache for passively-replicating chains.
}
```

#### `store.default(opts)`
Create a new default unichain, which is used for bootstrapping the creation of subsequent unichains. Options match those in `get`.

#### `store.get(opts)`
Create a new unichain. Options can be one of the following:
```js
{
  key: 0x1232..., // A Buffer representing a unichain key
  discoveryKey: 0x1232..., // A Buffer representing a unichain discovery key (must have been previously created by key)
  ...opts // All other options accepted by the unichain constructor
}
```

If `opts` is a Buffer, it will be interpreted as a unichain key.

#### `store.on('feed', feed, options)`

Emitted everytime a feed is loaded internally (ie, the first time get(key) is called).
Options will be the full options map passed to .get.

#### `store.replicate(isInitiator, [opts])`
Create a replication stream that will replicate all chains currently in memory in the chainstore instance.

When piped to another chainstore's replication stream, only those chains that are shared between the two chainstores will be successfully replicated.

#### `store.list()`
Returns a Map of all chains currently cached in memory. For each chain in memory, the map will contain the following entries:
```
{
  discoveryKey => chain,
  ...
}
```

#### `const namespacedStore = store.namespace('some-name')`
Create a "namespaced" chainstore that uses the same underlying storage as its parent, and mirrors the complete chainstore API. 

`namespacedStore.default` returns a different default chain, using the namespace as part of key generation, which makes it easier to bootstrap multiple data structures from the same chainstore. The general pattern is for all data structures to bootstrap themselves from their chainstore's default feed:
```js
const store = new Chainstore(ram)
const drive1 = new Bitdrive(store.namespace('drive1'))
const drive2 = new Bitdrive(store.namespace('drive2'))
```

Namespaces currently need to be saved separately outside of chainstore (as a mapping from key to namespace), so that data structures remain writable across restarts. Extending the above code, this might look like:
```js
async function getDrive (opts = {}) {
  let namespace = opts.key ? await lookupNamespace(opts.key) : await createNamespace()
  const namespacedChainstore = store.namespace(namespace)
  const drive = new Bitdrive(namespacedChainstore)
  await saveNamespace(drive.key, namespace)
}
```

#### `store.close(cb)`
Close all unichains previously generated by the chainstore.

### License
MIT

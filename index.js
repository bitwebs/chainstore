const BitProtocol = require('@web4/bit-protocol')
const Nanoresource = require('nanoresource/emitter')
const unichain = require('@web4/unichain')
const bitwebCrypto = require('@web4/crypto')
const bitEncoding = require('@web4/encoding')
const maybe = require('call-me-maybe')

const RefPool = require('refpool')
const deriveSeed = require('derive-key')
const derivedStorage = require('derived-key-storage')
const raf = require('random-access-file')

const MASTER_KEY_FILENAME = 'master_key'
const NAMESPACE = 'chainstore'
const NAMESPACE_SEPERATOR = ':'

class InnerChainstore extends Nanoresource {
  constructor (storage, opts = {}) {
    super()

    if (typeof storage === 'string') storage = defaultStorage(storage)
    if (typeof storage !== 'function') throw new Error('Storage should be a function or string')
    this.storage = storage

    this.opts = opts

    this._replicationStreams = []
    this.cache = new RefPool({
      maxSize: opts.cacheSize || 1000,
      close: chain => {
        chain.close(err => {
          if (err) this.emit('error', err)
        })
      }
    })

    // Generated in _open
    this._masterKey = opts.masterKey || null
    this._id = bitwebCrypto.randomBytes(8)

    // As discussed in https://github.com/andrewosh/chainstore/issues/20
    this.setMaxListeners(0)
  }

  // Nanoresource Methods

  _open (cb) {
    if (this._masterKey) return cb()
    const keyStorage = this.storage(MASTER_KEY_FILENAME)
    keyStorage.stat((err, st) => {
      if (err && err.code !== 'ENOENT') return cb(err)
      if (err || st.size < 32) {
        this._masterKey = bitwebCrypto.randomBytes(32)
        return keyStorage.write(0, this._masterKey, err => {
          if (err) return cb(err)
          keyStorage.close(cb)
        })
      }
      keyStorage.read(0, 32, (err, key) => {
        if (err) return cb(err)
        this._masterKey = key
        keyStorage.close(cb)
      })
    })
  }

  _close (cb) {
    let error = null
    for (const { stream } of this._replicationStreams) {
      stream.destroy()
    }
    if (!this.cache.size) return process.nextTick(cb, null)
    let remaining = this.cache.size
    for (const { value: chain } of this.cache.entries.values()) {
      chain.close(err => {
        if (err) error = err
        if (!--remaining) {
          if (error) return cb(error)
          return cb(null)
        }
      })
    }
  }

  // Private Methods

  _checkIfExists (dkey, cb) {
    dkey = encodeKey(dkey)
    if (this.cache.has(dkey)) return process.nextTick(cb, null, true)

    const chainStorage = this.storage([dkey.slice(0, 2), dkey.slice(2, 4), dkey, 'key'].join('/'))

    chainStorage.read(0, 32, (err, key) => {
      if (err) return cb(err)
      chainStorage.close(err => {
        if (err) return cb(err)
        if (!key) return cb(null, false)
        return cb(null, true)
      })
    })
  }

  _injectIntoReplicationStreams (chain) {
    for (const { stream, opts } of this._replicationStreams) {
      this._replicateChain(false, chain, stream, { ...opts })
    }
  }

  _replicateChain (isInitiator, chain, mainStream, opts) {
    if (!chain) return
    chain.ready(function (err) {
      if (err) return
      chain.replicate(isInitiator, {
        ...opts,
        stream: mainStream
      })
    })
  }

  _deriveSecret (namespace, name) {
    return deriveSeed(namespace, this._masterKey, name)
  }

  _generateKeyPair (name) {
    if (typeof name === 'string') name = Buffer.from(name)
    else if (!name) name = bitwebCrypto.randomBytes(32)

    const seed = this._deriveSecret(NAMESPACE, name)

    const keyPair = bitwebCrypto.keyPair(seed)
    const discoveryKey = bitwebCrypto.discoveryKey(keyPair.publicKey)
    return { name, publicKey: keyPair.publicKey, secretKey: keyPair.secretKey, discoveryKey }
  }

  _generateKeys (chainOpts) {
    if (!chainOpts) chainOpts = {}
    if (typeof chainOpts === 'string') chainOpts = Buffer.from(chainOpts, 'hex')
    if (Buffer.isBuffer(chainOpts)) chainOpts = { key: chainOpts }

    if (chainOpts.keyPair) {
      const publicKey = chainOpts.keyPair.publicKey
      const secretKey = chainOpts.keyPair.secretKey
      return {
        publicKey,
        secretKey,
        discoveryKey: bitwebCrypto.discoveryKey(publicKey),
        name: null
      }
    }
    if (chainOpts.key) {
      const publicKey = decodeKey(chainOpts.key)
      return {
        publicKey,
        secretKey: null,
        discoveryKey: bitwebCrypto.discoveryKey(publicKey),
        name: null
      }
    }
    if (chainOpts.default || chainOpts.name) {
      if (!chainOpts.name) throw new Error('If the default option is set, a name must be specified.')
      return this._generateKeyPair(chainOpts.name)
    }
    if (chainOpts.discoveryKey) {
      const discoveryKey = decodeKey(chainOpts.discoveryKey)
      return {
        publicKey: null,
        secretKey: null,
        discoveryKey,
        name: null
      }
    }
    return this._generateKeyPair(null)
  }

  // Public Methods

  isLoaded (chainOpts) {
    const generatedKeys = this._generateKeys(chainOpts)
    return this.cache.has(encodeKey(generatedKeys.discoveryKey))
  }

  isExternal (chainOpts) {
    const generatedKeys = this._generateKeys(chainOpts)
    const entry = this._cache.entry(encodeKey(generatedKeys.discoveryKey))
    if (!entry) return false
    return entry.refs !== 0
  }

  get (chainOpts = {}) {
    if (!this.opened) throw new Error('Chainstore.ready must be called before get.')

    const self = this

    const generatedKeys = this._generateKeys(chainOpts)
    const { publicKey, discoveryKey, secretKey } = generatedKeys
    const id = encodeKey(discoveryKey)

    const cached = this.cache.get(id)
    if (cached) return cached

    const storageRoot = [id.slice(0, 2), id.slice(2, 4), id].join('/')

    const keyStorage = derivedStorage(createStorage, (name, cb) => {
      if (name) {
        const res = this._generateKeyPair(name)
        if (discoveryKey && (!discoveryKey.equals((res.discoveryKey)))) {
          return cb(new Error('Stored an incorrect name.'))
        }
        return cb(null, res)
      }
      if (secretKey) return cb(null, generatedKeys)
      if (publicKey) return cb(null, { name: null, publicKey, secretKey: null })
      const err = new Error('Unknown key pair.')
      err.unknownKeyPair = true
      return cb(err)
    })

    const cacheOpts = { ...this.opts.cache }
    if (chainOpts.cache) {
      if (chainOpts.cache.data === false) delete cacheOpts.data
      if (chainOpts.cache.tree === false) delete cacheOpts.tree
    }
    if (cacheOpts.data) cacheOpts.data = cacheOpts.data.namespace()
    if (cacheOpts.tree) cacheOpts.tree = cacheOpts.tree.namespace()

    const chain = unichain(name => {
      if (name === 'key') return keyStorage.key
      if (name === 'secret_key') return keyStorage.secretKey
      return createStorage(name)
    }, publicKey, {
      ...this.opts,
      ...chainOpts,
      cache: cacheOpts,
      createIfMissing: !!publicKey
    })

    this.cache.set(id, chain)
    chain.ifAvailable.wait()

    var errored = false
    chain.once('error', onerror)
    chain.once('ready', onready)
    chain.once('close', onclose)

    return chain

    function onready () {
      if (errored) return
      self.emit('feed', chain, chainOpts)
      chain.removeListener('error', onerror)
      self._injectIntoReplicationStreams(chain)
      // TODO: nexttick here needed? prob not, just legacy
      process.nextTick(() => chain.ifAvailable.continue())
    }

    function onerror (err) {
      errored = true
      chain.ifAvailable.continue()
      self.cache.delete(id)
      if (err.unknownKeyPair) {
        // If an error occurs during creation by discovery key, then that chain does not exist on disk.
        // TODO: This should not throw, but should propagate somehow.
      }
    }

    function onclose () {
      self.cache.delete(id)
    }

    function createStorage (name) {
      return self.storage(storageRoot + '/' + name)
    }
  }

  replicate (isInitiator, chains, replicationOpts = {}) {
    const self = this

    const finalOpts = { ...this.opts, ...replicationOpts }
    const mainStream = replicationOpts.stream || new BitProtocol(isInitiator, { ...finalOpts })
    var closed = false

    for (const chain of chains) {
      this._replicateChain(isInitiator, chain, mainStream, { ...finalOpts })
    }

    mainStream.on('discovery-key', ondiscoverykey)
    mainStream.on('finish', onclose)
    mainStream.on('end', onclose)
    mainStream.on('close', onclose)

    const streamState = { stream: mainStream, opts: finalOpts }
    this._replicationStreams.push(streamState)

    return mainStream

    function ondiscoverykey (dkey) {
      // Get will automatically add the chain to all replication streams.
      self._checkIfExists(dkey, (err, exists) => {
        if (closed) return
        if (err || !exists) return mainStream.close(dkey)
        const passiveChain = self.get({ discoveryKey: dkey })
        self._replicateChain(false, passiveChain, mainStream, { ...finalOpts })
      })
    }

    function onclose () {
      if (!closed) {
        self._replicationStreams.splice(self._replicationStreams.indexOf(streamState), 1)
        closed = true
      }
    }
  }
}

class Chainstore extends Nanoresource {
  constructor (storage, opts = {}) {
    super()

    this.storage = storage
    this.name = opts.name || 'default'
    this.inner = opts.inner || new InnerChainstore(storage, opts)
    this.cache = this.inner.cache
    this.store = this // Backwards-compat for NamespacedChainstore

    this._parent = opts.parent
    this._isNamespaced = !!opts.name
    this._openedChains = new Map()

    const onfeed = feed => this.emit('feed', feed)
    const onerror = err => this.emit('error', err)
    this.inner.on('feed', onfeed)
    this.inner.on('error', onerror)
    this._unlisten = () => {
      this.inner.removeListener('feed', onfeed)
      this.inner.removeListener('error', onerror)
    }
  }

  ready (cb) {
    return maybe(cb, new Promise((resolve, reject) => {
      this.open(err => {
        if (err) return reject(err)
        return resolve()
      })
    }))
  }

  // Nanoresource Methods

  _open (cb) {
    return this.inner.open(cb)
  }

  _close (cb) {
    this._unlisten()
    if (!this._parent) return this.inner.close(cb)
    for (const dkey of this._openedChains) {
      this.cache.decrement(dkey)
    }
    return process.nextTick(cb, null)
  }

  // Private Methods

  _maybeIncrement (chain) {
    const id = encodeKey(chain.discoveryKey)
    if (this._openedChains.has(id)) return
    this._openedChains.set(id, chain)
    this.cache.increment(id)
  }

  // Public Methods

  get (chainOpts = {}) {
    if (Buffer.isBuffer(chainOpts)) chainOpts = { key: chainOpts }
    const chain = this.inner.get(chainOpts)
    this._maybeIncrement(chain)
    return chain
  }

  default (chainOpts = {}) {
    if (Buffer.isBuffer(chainOpts)) chainOpts = { key: chainOpts }
    return this.get({ ...chainOpts, name: this.name })
  }

  namespace (name) {
    if (!name) name = bitwebCrypto.randomBytes(32)
    if (Buffer.isBuffer(name)) name = name.toString('hex')
    name = this._isNamespaced ? this.name + NAMESPACE_SEPERATOR + name : name
    return new Chainstore(this.storage, {
      inner: this.inner,
      parent: this,
      name
    })
  }

  replicate (isInitiator, opts) {
    const chains = !this._parent ? allReferenced(this.cache) : this._openedChains.values()
    return this.inner.replicate(isInitiator, chains, opts)
  }

  isLoaded (chainOpts) {
    return this.inner.isLoaded(chainOpts)
  }

  isExternal (chainOpts) {
    return this.inner.isExternal(chainOpts)
  }

  list () {
    return new Map([...this._openedChains])
  }
}

function * allReferenced (cache) {
  for (const entry of cache.entries.values()) {
    if (entry.refs > 0) yield entry.value
    continue
  }
}

function encodeKey (key) {
  return Buffer.isBuffer(key) ? bitEncoding.encode(key) : key
}

function decodeKey (key) {
  return (typeof key === 'string') ? bitEncoding.decode(key) : key
}

function defaultStorage (dir) {
  return function (name) {
    try {
      var lock = name.endsWith('/bitfield') ? require('fd-lock') : null
    } catch (err) {}
    return raf(name, { directory: dir, lock: lock })
  }
}

module.exports = Chainstore

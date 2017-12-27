'use strict'

const Hp = require('hemera-plugin')
const StorePattern = require('hemera-store/pattern')
const rethinkdbdash = require('rethinkdbdash')

function hemeraRethinkdbStore(hemera, opts, done) {
  const topic = 'rethinkdb-store'
  const Joi = hemera.joi

  const rethinkdb = rethinkdbdash(opts.rethinkdb)

  hemera.decorate('rethinkdb', {
    driver: rethinkdb
  })

  // Gracefully shutdown
  hemera.ext('onClose', (ctx, done) => {
    hemera.log.debug('Rethinkdb connection closed!')
    done()
  })

  /**
   * Helper functions
   */

  hemera.add(
    {
      topic,
      cmd: 'createDatabase',
      databaseName: Joi.string().default(opts.rethinkdb.db)
    },
    function(req, reply) {
      return rethinkdb.dbCreate(req.databaseName).run(reply)
    }
  )

  hemera.add(
    {
      topic,
      cmd: 'removeDatabase',
      databaseName: Joi.string().default(opts.rethinkdb.db)
    },
    function(req, reply) {
      return rethinkdb.dbDrop(req.databaseName).run(reply)
    }
  )

  hemera.add(
    {
      topic,
      cmd: 'createTable',
      collection: Joi.string().required(),
      databaseName: Joi.string().default(opts.rethinkdb.db)
    },
    function(req, reply) {
      return rethinkdb
        .db(req.databaseName)
        .tableCreate(req.collection, { primaryKey: 'id' })
        .run(reply)
    }
  )

  hemera.add(
    {
      topic,
      cmd: 'removeTable',
      collection: Joi.string().required(),
      databaseName: Joi.string().default(opts.rethinkdb.db)
    },
    function(req, reply) {
      rethinkdb
        .db(req.databaseName)
        .tableDrop(req.collection)
        .run(reply)
    }
  )

  hemera.add(
    {
      topic,
      cmd: 'truncateTable',
      collection: Joi.string().required(),
      databaseName: Joi.string().default(opts.rethinkdb.db)
    },
    function(req, reply) {
      rethinkdb
        .db(req.databaseName)
        .table(req.collection)
        .delete()
        .run(reply)
    }
  )

  /**
   * Special functions
   */

  hemera.add(
    {
      topic,
      cmd: 'changes',
      collection: Joi.string().required(),
      query: Joi.object(),
      databaseName: Joi.string().default(opts.rethinkdb.db),
      options: Joi.object()
        .keys({
          fields: Joi.alternatives().try(Joi.object(), Joi.array()),
          orderBy: Joi.alternatives().try(
            Joi.object(),
            Joi.array(),
            Joi.string()
          ),
          offset: Joi.number().integer(),
          limit: Joi.number()
            .integer()
            .default(1)
        })
        .default({})
    },
    function(req, reply) {
      let cursor = rethinkdb.db(req.databaseName).table(req.collection)

      if (req.query) {
        cursor = cursor.filter(req.query)
      }

      if (req.options.limit) {
        cursor = cursor.limit(req.options.limit)
      }
      if (req.options.offset) {
        cursor = cursor.skip(req.options.offset)
      }
      if (req.options.fields) {
        cursor = cursor.pluck(req.options.fields)
      }
      if (req.options.orderBy) {
        cursor = cursor.orderBy(req.options.orderBy)
      }

      cursor
        .changes()
        .run()
        .then(cursor => {
          return cursor.next().then(changes => reply(null, changes))
        })
        .catch(err => reply(err))

      // to inform about successful subscription
      reply(null, true)
    }
  )

  /**
   * Store interface
   */

  hemera.add(StorePattern.create(topic), function(req, reply) {
    const databaseName = req.databaseName || opts.rethinkdb.db
    rethinkdb
      .db(databaseName)
      .table(req.collection)
      .insert(req.data)
      .run(reply)
  })

  hemera.add(StorePattern.update(topic), function(req, reply) {
    const databaseName = req.databaseName || opts.rethinkdb.db
    rethinkdb
      .db(databaseName)
      .table(req.collection)
      .filter(req.query)
      .update(req.data)
      .run(reply)
  })

  hemera.add(StorePattern.updateById(topic), function(req, reply) {
    const databaseName = req.databaseName || opts.rethinkdb.db
    rethinkdb
      .db(databaseName)
      .table(req.collection)
      .get(req.id)
      .update(req.data)
      .run(reply)
  })

  hemera.add(StorePattern.remove(topic), function(req, reply) {
    const databaseName = req.databaseName || opts.rethinkdb.db
    rethinkdb
      .db(databaseName)
      .table(req.collection)
      .filter(req.query)
      .delete()
      .run(reply)
  })

  hemera.add(StorePattern.removeById(topic), function(req, reply) {
    const databaseName = req.databaseName || opts.rethinkdb.db
    rethinkdb
      .db(databaseName)
      .table(req.collection)
      .get(req.id)
      .delete()
      .run(reply)
  })

  hemera.add(StorePattern.replace(topic), function(req, reply) {
    const databaseName = req.databaseName || opts.rethinkdb.db
    rethinkdb
      .db(databaseName)
      .table(req.collection)
      .filter(req.query)
      .replace(req.data)
      .run(reply)
  })

  hemera.add(StorePattern.replaceById(topic), function(req, reply) {
    const databaseName = req.databaseName || opts.rethinkdb.db
    rethinkdb
      .db(databaseName)
      .table(req.collection)
      .get(req.id)
      .replace(req.data)
      .run(reply)
  })

  hemera.add(StorePattern.findById(topic), function(req, reply) {
    const databaseName = req.databaseName || opts.rethinkdb.db
    rethinkdb
      .db(databaseName)
      .table(req.collection)
      .get(req.id)
      .run(reply)
  })

  hemera.add(StorePattern.count(topic), function(req, reply) {
    const databaseName = req.databaseName || opts.rethinkdb.db
    rethinkdb
      .db(databaseName)
      .table(req.collection)
      .filter(req.query)
      .count()
      .run(reply)
  })

  hemera.add(StorePattern.exists(topic), function(req, reply) {
    const databaseName = req.databaseName || opts.rethinkdb.db
    rethinkdb
      .db(databaseName)
      .table(req.collection)
      .filter(req.query)
      .count()
      .run((err, value) => reply(err, !!value))
  })

  hemera.add(StorePattern.find(topic), function(req, reply) {
    const databaseName = req.databaseName || opts.rethinkdb.db
    let cursor = rethinkdb
      .db(databaseName)
      .table(req.collection)
      .filter(req.query)

    if (req.options) {
      if (req.options.limit) {
        cursor = cursor.limit(opts.limit)
      }
      if (req.options.offset) {
        cursor = cursor.skip(opts.offset)
      }
      if (req.options.fields) {
        cursor = cursor.pluck(req.options.fields)
      }
      if (req.options.orderBy) {
        cursor = cursor.orderBy(req.options.orderBy)
      }
    }

    cursor.run({ cursor: true }, (err, result) => {
      if (err) {
        return reply(err)
      }
      result.toArray(reply)
    })
  })

  done()
}

module.exports = Hp(hemeraRethinkdbStore, {
  hemera: '>=3',
  name: require('./package.json').name,
  dependencies: ['hemera-joi'],
  options: {
    payloadValidator: 'hemera-joi',
    rethinkdb: {
      pool: true,
      cursor: true
    }
  }
})

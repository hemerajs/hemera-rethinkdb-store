'use strict'

const Hemera = require('nats-hemera')
const HemeraRethinkdbStore = require('./..')
const HemeraJoi = require('hemera-joi')
const Code = require('code')
const Nats = require('nats')
const HemeraTestsuite = require('hemera-testsuite')

const expect = Code.expect

describe('Store interface', function() {
  let PORT = 6242
  var authUrl = 'nats://localhost:' + PORT

  let server
  let hemera
  let testDatabase = 'test'
  let testCollection = 'users'
  let topic = 'rethinkdb-store'

  function bootstrap(done) {
    hemera.act(
      {
        topic,
        cmd: 'createTable',
        collection: testCollection
      },
      function(err, resp) {
        if (err) throw err
        done()
      }
    )
  }

  function clean(done) {
    hemera.act(
      {
        topic,
        cmd: 'removeTable',
        collection: testCollection
      },
      function(err, resp) {
        if (err) throw err
        done()
      }
    )
  }

  before(function(done) {
    server = HemeraTestsuite.start_server(PORT, () => {
      const nats = Nats.connect(authUrl)
      hemera = new Hemera(nats, {
        crashOnFatal: false
      })
      hemera.use(HemeraJoi)
      hemera.use(HemeraRethinkdbStore, {
        rethinkdb: {
          db: testDatabase
        }
      })
      hemera.ready(function() {
        bootstrap(done)
      })
    })
  })

  after(function(done) {
    clean(() => {
      hemera.close()
      server.kill()
      done()
    })
  })

  it('Create', function(done) {
    hemera.act(
      {
        topic,
        cmd: 'create',
        collection: testCollection,
        data: {
          name: 'peter'
        }
      },
      function(err, resp) {
        expect(err).to.be.not.exists()
        expect(resp).to.be.an.object()

        done()
      }
    )
  })

  it('Find', function(done) {
    hemera.act(
      {
        topic,
        cmd: 'find',
        collection: testCollection,
        query: {}
      },
      function(err, resp) {
        expect(err).to.be.not.exists()
        expect(resp).to.be.an.array()

        done()
      }
    )
  })

  it('FindById', function(done) {
    hemera.act(
      {
        topic,
        cmd: 'create',
        collection: testCollection,
        data: {
          name: 'peter'
        }
      },
      function(err, resp) {
        expect(err).to.be.not.exists()
        expect(resp).to.be.an.object()

        hemera.act(
          {
            topic,
            cmd: 'findById',
            collection: testCollection,
            id: resp.generated_keys[0]
          },
          function(err, resp2) {
            expect(err).to.be.not.exists()
            expect(resp2).to.be.an.object()
            expect(resp2.name).to.be.equals('peter')
            expect(resp2.id).to.be.equals(resp.generated_keys[0])
            done()
          }
        )
      }
    )
  })

  it('Update', function(done) {
    hemera.act(
      {
        topic,
        cmd: 'create',
        collection: testCollection,
        data: {
          name: 'peter'
        }
      },
      function(err, resp) {
        expect(err).to.be.not.exists()
        expect(resp).to.be.an.object()

        hemera.act(
          {
            topic,
            cmd: 'update',
            collection: testCollection,
            data: {
              name: 'peter2'
            },
            query: {
              id: resp.generated_keys[0]
            }
          },
          function(err, resp) {
            expect(err).to.be.not.exists()
            expect(resp).to.be.an.object()
            expect(resp.replaced).to.be.equals(1)
            done()
          }
        )
      }
    )
  })

  it('UpdateById', function(done) {
    hemera.act(
      {
        topic,
        cmd: 'create',
        collection: testCollection,
        data: {
          name: 'peter'
        }
      },
      function(err, resp) {
        expect(err).to.be.not.exists()
        expect(resp).to.be.an.object()

        hemera.act(
          {
            topic,
            cmd: 'updateById',
            collection: testCollection,
            data: {
              name: 'peter2'
            },
            id: resp.generated_keys[0]
          },
          function(err, resp) {
            expect(err).to.be.not.exists()
            expect(resp).to.be.an.object()
            expect(resp.replaced).to.be.equals(1)
            done()
          }
        )
      }
    )
  })

  it('Remove', function(done) {
    hemera.act(
      {
        topic,
        cmd: 'create',
        collection: testCollection,
        data: {
          name: 'peter'
        }
      },
      function(err, resp) {
        expect(err).to.be.not.exists()
        expect(resp).to.be.an.object()

        hemera.act(
          {
            topic,
            cmd: 'remove',
            collection: testCollection,
            data: {
              name: 'peter2'
            },
            query: {
              id: resp.generated_keys[0]
            }
          },
          function(err, resp) {
            expect(err).to.be.not.exists()
            expect(resp).to.be.an.object()
            expect(resp.deleted).to.be.equals(1)
            done()
          }
        )
      }
    )
  })

  it('RemoveById', function(done) {
    hemera.act(
      {
        topic,
        cmd: 'create',
        collection: testCollection,
        data: {
          name: 'peter'
        }
      },
      function(err, resp) {
        expect(err).to.be.not.exists()
        expect(resp).to.be.an.object()

        hemera.act(
          {
            topic,
            cmd: 'removeById',
            collection: testCollection,
            data: {
              name: 'peter2'
            },
            id: resp.generated_keys[0]
          },
          function(err, resp) {
            expect(err).to.be.not.exists()
            expect(resp).to.be.an.object()
            expect(resp.deleted).to.be.equals(1)
            done()
          }
        )
      }
    )
  })

  it('Replace', function(done) {
    hemera.act(
      {
        topic,
        cmd: 'create',
        collection: testCollection,
        data: {
          name: 'peter'
        }
      },
      function(err, resp) {
        expect(err).to.be.not.exists()
        expect(resp).to.be.an.object()

        hemera.act(
          {
            topic,
            cmd: 'replace',
            collection: testCollection,
            data: {
              id: resp.generated_keys[0],
              name: 'peter2'
            },
            query: {
              id: resp.generated_keys[0]
            }
          },
          function(err, resp) {
            expect(err).to.be.not.exists()
            expect(resp).to.be.an.object()
            expect(resp.replaced).to.be.equals(1)
            done()
          }
        )
      }
    )
  })

  it('ReplaceById', function(done) {
    hemera.act(
      {
        topic,
        cmd: 'create',
        collection: testCollection,
        data: {
          name: 'peter'
        }
      },
      function(err, resp) {
        expect(err).to.be.not.exists()
        expect(resp).to.be.an.object()

        hemera.act(
          {
            topic,
            cmd: 'replaceById',
            collection: testCollection,
            data: {
              id: resp.generated_keys[0],
              name: 'peter2'
            },
            id: resp.generated_keys[0]
          },
          function(err, resp) {
            expect(err).to.be.not.exists()
            expect(resp).to.be.an.object()
            expect(resp.replaced).to.be.equals(1)
            done()
          }
        )
      }
    )
  })

  it('Count', function(done) {
    hemera.act(
      {
        topic,
        cmd: 'create',
        collection: testCollection,
        data: {
          name: 'peter'
        }
      },
      function(err, resp) {
        expect(err).to.be.not.exists()
        expect(resp).to.be.an.object()

        hemera.act(
          {
            topic,
            cmd: 'count',
            collection: testCollection,
            query: {
              id: resp.generated_keys[0]
            }
          },
          function(err, resp) {
            expect(err).to.be.not.exists()
            expect(resp).to.be.a.number()
            expect(resp).to.be.equals(1)
            done()
          }
        )
      }
    )
  })

  it('Exists', function(done) {
    hemera.act(
      {
        topic,
        cmd: 'create',
        collection: testCollection,
        data: {
          name: 'peter'
        }
      },
      function(err, resp) {
        expect(err).to.be.not.exists()
        expect(resp).to.be.an.object()

        hemera.act(
          {
            topic,
            cmd: 'exists',
            collection: testCollection,
            query: {
              id: resp.generated_keys[0]
            }
          },
          function(err, resp) {
            expect(err).to.be.not.exists()
            expect(resp).to.be.a.boolean()
            expect(resp).to.be.equals(true)
            done()
          }
        )
      }
    )
  })

  it('Changes', function(done) {
    let changes
    hemera.act(
      {
        topic,
        cmd: 'changes',
        collection: testCollection,
        query: {
          name: 'changes'
        },
        maxMessages$: -1
      },
      function(err, resp) {
        if (resp === true) {
          //listen ACK
          hemera.act(
            {
              topic,
              cmd: 'create',
              collection: testCollection,
              data: {
                name: 'changes'
              }
            },
            err => {
              setTimeout(() => {
                expect(changes.new_val.name).to.be.equals('changes')
                expect(changes.old_val).to.be.equals(null)
                done()
              }, 200)
            }
          )
        } else {
          changes = resp
        }
      }
    )
  })
})

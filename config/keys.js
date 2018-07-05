const keys = {
    mongo: 'mongodb://admin:admin@localhost:27017/users',
    cassandra: {
        contactPoints: ['127.0.0.1'],
        user: 'cassandra',
        pwd: 'cassandra'
    },
    redis:{
        db: 0,
        pwd: 'admin'
    },
    postgres: 'postgres://cubex:cubex@localhost:5432/users'
}

module.exports = keys